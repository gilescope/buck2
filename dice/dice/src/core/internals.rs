/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use dupe::Dupe;
use pagable::DataKey;

use crate::api::key::InvalidationSourcePriority;
use crate::api::storage_type::StorageType;
use crate::arc::Arc;
use crate::core::graph::introspection::VersionedGraphIntrospectable;
use crate::core::graph::nodes::VersionedGraphNode;
use crate::core::graph::storage::InvalidateKind;
use crate::core::graph::storage::ValueReusable;
use crate::core::graph::storage::VersionedGraph;
use crate::core::graph::types::VersionedGraphKey;
use crate::core::graph::types::VersionedGraphResult;
use crate::core::versions::VersionEpoch;
use crate::core::versions::VersionTracker;
use crate::core::versions::introspection::VersionIntrospectable;
use crate::deps::graph::SeriesParallelDeps;
use crate::dice::PagableNodeCounts;
use crate::epoch::cache::SharedCache;
use crate::epoch::cache::TransactionResult;
use crate::epoch::task::dice::DiceTask;
use crate::key::DiceKey;
use crate::metrics::Metrics;
use crate::updater::ChangeType;
use crate::value::DiceComputedValue;
use crate::value::DiceValidValue;
use crate::value::TrackedInvalidationPaths;
use crate::versions::VersionNumber;

/// Core state of DICE, holding the actual graph and version information
#[derive(allocative::Allocative)]
pub(super) struct CoreState {
    version_tracker: VersionTracker,
    graph: VersionedGraph,
    pending_termination_tasks: Vec<DiceTask>,
}

/// `CoreState::pagable_status` result. Holds raw `DiceKey`s; the caller resolves
/// them to key types off the core-state thread.
#[derive(Debug)]
pub(crate) struct PagableStatusRaw {
    /// Includes vacant/in-progress nodes, so `>= counts.resident + counts.paged_out`.
    pub(crate) total_nodes: usize,
    pub(crate) counts: PagableNodeCounts,
    /// Per-key-type breakdown source; lengths equal `counts.resident` / `counts.paged_out`.
    pub(crate) resident: Vec<DiceKey>,
    pub(crate) paged_out: Vec<DiceKey>,
}

/// One node eligible for pressure eviction, from
/// `CoreState::pressure_eviction_candidates`. The value is an `Arc` dupe; the
/// caller sizes/serializes it off the state thread.
pub(crate) struct PressureCandidate {
    pub(crate) key: DiceKey,
    pub(crate) value: DiceValidValue,
    /// `Some` = bytes already on disk; eviction needs no re-serialization.
    pub(crate) data_key: Option<DataKey>,
    /// Coldness rank (older = colder); see `OccupiedGraphNode::last_verified_begin`.
    pub(crate) last_verified_begin: VersionNumber,
}

impl CoreState {
    pub(super) fn new() -> Self {
        Self {
            version_tracker: VersionTracker::new(),
            graph: VersionedGraph::new(),
            pending_termination_tasks: Vec::new(),
        }
    }

    pub(super) fn update_state(
        &mut self,
        updates: impl IntoIterator<Item = (DiceKey, ChangeType, InvalidationSourcePriority)>,
    ) -> VersionNumber {
        let version_update = self.version_tracker.write();
        let v = version_update.version();

        let mut changes_recorded = false;
        for (key, change, invalidation_priority) in updates {
            changes_recorded |= self.graph.invalidate(
                VersionedGraphKey::new(v, key),
                match change {
                    ChangeType::Invalidate => InvalidateKind::ForceDirty,
                    ChangeType::UpdateValue(v, s) => InvalidateKind::Update(v, s),
                },
                invalidation_priority,
            );
        }
        if changes_recorded {
            version_update.commit()
        } else {
            version_update.undo()
        }
    }

    pub(super) fn ctx_at_version(&mut self, v: VersionNumber) -> (VersionEpoch, SharedCache) {
        self.version_tracker.at(v)
    }

    pub(super) fn current_version(&self) -> VersionNumber {
        self.version_tracker.current()
    }

    pub(super) fn drop_ctx_at_version(&mut self, v: VersionNumber) {
        if let Some(evicted_cache) = self.version_tracker.drop_at_version(v) {
            self.pending_termination_tasks
                .retain(|task| task.is_pending());
            self.pending_termination_tasks
                .extend(evicted_cache.cancel_pending_tasks());
        }
    }

    pub(super) fn lookup_key(&mut self, key: VersionedGraphKey) -> VersionedGraphResult {
        self.graph.get(key)
    }

    pub(super) fn update_computed(
        &mut self,
        key: VersionedGraphKey,
        epoch: VersionEpoch,
        storage: StorageType,
        value: DiceValidValue,
        reusability: ValueReusable,
        deps: Arc<SeriesParallelDeps>,
        invalidation_paths: TrackedInvalidationPaths,
    ) -> TransactionResult<DiceComputedValue> {
        if self.version_tracker.is_cancelled(key.v, epoch) {
            TransactionResult::make_cancelled()
        } else {
            TransactionResult::ok(
                self.graph
                    .update(key, value, reusability, deps, storage, invalidation_paths)
                    .0,
            )
        }
    }

    pub(super) fn get_tasks_pending_cancellation(&mut self) -> Vec<DiceTask> {
        self.pending_termination_tasks
            .retain(|task| task.is_pending());

        self.pending_termination_tasks.clone()
    }

    pub(super) fn unstable_drop_everything(&mut self) {
        self.version_tracker.clear();
        self.graph.clear();
    }

    /// Evict in-memory values for the given nodes, marking them as paged out
    /// with their `DataKey`s. Skips nodes that are missing, vacant, or injected.
    ///
    /// Checked: each entry carries the value that was serialized, and the node
    /// is only paged out if it still holds that exact value (pointer identity).
    /// Pressure eviction runs while builds are live, so a node can be
    /// recomputed between serialization and this message arriving - blindly
    /// paging out would pair the new value's node with stale on-disk bytes.
    pub(super) fn evict_keys(&mut self, keys: Vec<(DiceKey, DataKey, DiceValidValue)>) {
        for (key, data_key, serialized) in keys {
            if let Some(mut node) = self.graph.node_mut(key) {
                if let VersionedGraphNode::Occupied(occ) = &mut *node {
                    match occ.val().as_hydrated() {
                        Some(current) if current.ptr_eq(&serialized) => occ.set_paged_out(data_key),
                        _ => {}
                    }
                }
            }
        }
    }

    /// The active transactions' caches, for off-state-thread inspection (the
    /// cache structures are concurrent). Cheap: clones of `Arc`d handles.
    pub(super) fn active_caches(&self) -> Vec<SharedCache> {
        self.version_tracker
            .currently_active()
            .map(|(_refcount, cache)| cache.dupe())
            .collect()
    }

    /// Nodes eligible for pressure eviction: occupied, hydrated, not in
    /// `referenced` (keys referenced by an active transaction's cache - the
    /// cache pins the value's `Arc`, so evicting those frees nothing), and
    /// with no rdep in `pending` (an in-flight parent may read the value
    /// straight back - thrash). The caller builds both sets off this thread
    /// from `active_caches()` - scanning the caches here would stall the
    /// core-state thread, which is the build's bottleneck (measured: +75%
    /// wall on an analysis-heavy leg at a 5s poll). The sets may be slightly
    /// stale; evicting a just-completed value is harmless (it stays pinned by
    /// its cache until the transaction drops, then hydrates on demand).
    pub(super) fn pressure_eviction_candidates(
        &self,
        referenced: &crate::HashSet<DiceKey>,
        pending: &crate::HashSet<DiceKey>,
    ) -> Vec<PressureCandidate> {
        // Enumerated from the graph's page-out candidate set (resident and never
        // paged out), same source as `keys_to_page_out`, rather than scanning
        // every node.
        self.graph
            .page_out_candidates()
            .iter()
            .filter_map(|index| {
                let key = DiceKey {
                    index: index as u32,
                };
                let VersionedGraphNode::Occupied(occ) = self.graph.nodes().get(&key)? else {
                    return None;
                };
                let value = occ.val().as_hydrated()?;
                if referenced.contains(&key) {
                    return None;
                }
                if occ.rdeps().any(|r| pending.contains(&r)) {
                    return None;
                }
                Some(PressureCandidate {
                    key,
                    value: value.dupe(),
                    data_key: occ.val().data_key(),
                    last_verified_begin: occ.last_verified_begin(),
                })
            })
            .collect()
    }

    /// Persist support: extract every node's snapshot metadata. Runs on the
    /// state thread; everything returned is plain data + Arc clones.
    pub(super) fn persist_extract(
        &self,
    ) -> (VersionNumber, Vec<crate::persist::PersistNodeExtract>) {
        use crate::persist::PersistNodeExtract;
        use crate::persist::PersistValueExtract;
        let mut out = Vec::with_capacity(self.graph.nodes().len());
        for (key, node) in self.graph.nodes() {
            match node {
                VersionedGraphNode::Occupied(occ) => {
                    let (deps, ranges, dirtied) = occ.parts_for_persist();
                    let value = match (occ.val().data_key(), occ.val().as_hydrated()) {
                        (Some(dk), _) => PersistValueExtract::Paged(dk),
                        (None, Some(v)) => PersistValueExtract::Hydrated(v.dupe()),
                        (None, None) => continue, // unreachable by PagableNodeValue invariant
                    };
                    out.push(PersistNodeExtract::Occupied {
                        key: *key,
                        deps: deps.iter_keys().collect(),
                        verified_ranges: ranges.clone(),
                        dirtied_history: dirtied.clone(),
                        value,
                    });
                }
                VersionedGraphNode::Injected(inj) => {
                    let (first_valid, value) = inj.latest_for_persist();
                    out.push(PersistNodeExtract::Injected {
                        key: *key,
                        first_valid_version: first_valid,
                        value: value.dupe(),
                    });
                }
                VersionedGraphNode::Vacant(_) => {}
            }
        }
        (self.version_tracker.current(), out)
    }

    /// Persist support: install reconstructed nodes and resume version
    /// numbering at the snapshot's version.
    pub(super) fn persist_install(
        &mut self,
        nodes: Vec<(DiceKey, VersionedGraphNode)>,
        at_version: VersionNumber,
    ) {
        self.graph.install_persisted_nodes(nodes, at_version);
        self.version_tracker.fast_forward_for_persist(at_version);
    }

    /// Mark nodes that page-out considered but could not serialize, so they are
    /// not offered as page-out candidates again (including after a recompute).
    pub(super) fn mark_non_pageable(&mut self, keys: Vec<DiceKey>) {
        for key in keys {
            if let Some(mut node) = self.graph.node_mut(key) {
                if let VersionedGraphNode::Occupied(occ) = &mut *node {
                    occ.mark_non_pageable();
                }
            }
        }
    }

    /// Returns resident nodes that have never been paged out — the page-out
    /// candidates. Enumerated from the graph's candidate set rather than scanning
    /// every node.
    pub(super) fn keys_to_page_out(&self) -> Vec<(DiceKey, DiceValidValue)> {
        self.graph
            .page_out_candidates()
            .iter()
            .filter_map(|index| {
                let key = DiceKey {
                    index: index as u32,
                };
                let VersionedGraphNode::Occupied(occ) = self.graph.nodes().get(&key)? else {
                    return None;
                };
                Some((key, occ.val().as_hydrated()?.dupe()))
            })
            .collect()
    }

    /// Returns the list of `(DiceKey, DataKey)` pairs for every paged-out
    /// `OccupiedGraphNode`. The caller performs the actual (async) hydration
    /// outside the core state thread and sends rehydrate messages back.
    pub(super) fn paged_out_keys(&self) -> Vec<(DiceKey, DataKey)> {
        let mut keys = Vec::new();
        for (key, node) in self.graph.nodes() {
            let VersionedGraphNode::Occupied(occ) = node else {
                continue;
            };
            if occ.val().as_hydrated().is_some() {
                continue;
            }
            let Some(data_key) = occ.val().data_key() else {
                continue;
            };
            keys.push((*key, data_key));
        }
        keys
    }

    /// Classify each `OccupiedGraphNode` as resident (value in memory) or paged
    /// out (only a `DataKey` left). Occupied-but-neither can't happen (a
    /// `PagableNodeValue` always holds exactly one) and is omitted from both lists.
    pub(super) fn pagable_status(&self) -> PagableStatusRaw {
        let mut resident = Vec::new();
        let mut paged_out = Vec::new();
        for (key, node) in self.graph.nodes() {
            let VersionedGraphNode::Occupied(occ) = node else {
                continue;
            };
            if occ.val().as_hydrated().is_some() {
                resident.push(*key);
            } else if occ.val().data_key().is_some() {
                paged_out.push(*key);
            }
        }
        let counts = self.graph.pagable_node_counts();
        debug_assert_eq!(resident.len(), counts.resident, "resident count drifted");
        debug_assert_eq!(paged_out.len(), counts.paged_out, "paged-out count drifted");
        debug_assert_eq!(
            self.graph.page_out_candidates().count(),
            counts.candidates,
            "candidate count drifted",
        );
        PagableStatusRaw {
            total_nodes: self.graph.nodes().len(),
            counts,
            resident,
            paged_out,
        }
    }

    pub(super) fn pagable_node_counts(&self) -> PagableNodeCounts {
        self.graph.pagable_node_counts()
    }

    /// Replaces the paged-out value at `key` with its hydrated form. No-op if the node
    /// is missing, vacant, injected, or already hydrated.
    pub(super) fn rehydrate(&mut self, key: DiceKey, value: DiceValidValue) {
        if let Some(mut node) = self.graph.node_mut(key) {
            if let VersionedGraphNode::Occupied(occ) = &mut *node {
                occ.rehydrate(value);
            }
        }
    }

    /// Returns some metrics about the current state of DICE. Don't do expensive things here.
    pub(super) fn metrics(&self) -> Metrics {
        let mut active_transaction_count = 0;

        let currently_active = self.version_tracker.currently_active();
        for active in currently_active {
            active_transaction_count += active.0;
        }

        Metrics {
            key_count: self.graph.nodes().len(),
            active_transaction_count: active_transaction_count as u32, // probably won't support more than u32 transactions
        }
    }

    pub(super) fn introspection(&self) -> (VersionedGraphIntrospectable, VersionIntrospectable) {
        let graph = self.graph.introspect();
        let version_data = self.version_tracker.introspect();

        (graph, version_data)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use allocative::Allocative;
    use async_trait::async_trait;
    use derive_more::Display;
    use dice_futures::cancellation::CancellationContext;
    use dice_futures::spawner::TokioSpawner;
    use dupe::Dupe;
    use futures::FutureExt;
    use pagable::Pagable;
    use pagable::pagable_typetag;
    use tokio::sync::Semaphore;

    use crate::DiceKeyDyn;
    use crate::api::computations::DiceComputations;
    use crate::api::key::InvalidationSourcePriority;
    use crate::api::key::Key;
    use crate::api::key::NoValueSerialize;
    use crate::api::key::ValueSerialize;
    use crate::arc::Arc;
    use crate::core::graph::types::VersionedGraphKey;
    use crate::core::internals::CoreState;
    use crate::core::internals::StorageType;
    use crate::core::internals::ValueReusable;
    use crate::deps::graph::SeriesParallelDeps;
    use crate::epoch::cache::SharedCacheInsert;
    use crate::epoch::cache::TransactionCancelled;
    use crate::epoch::task::dice::DiceTask;
    use crate::epoch::task::dice::testing_helpers::make_completed_task;
    use crate::epoch::task::spawn_dice_task;
    use crate::key::DiceKey;
    use crate::updater::ChangeType;
    use crate::value::DiceKeyValue;
    use crate::value::DiceValidValue;
    use crate::value::TrackedInvalidationPaths;
    use crate::versions::VersionNumber;

    /// Checked eviction: a node holding a different value (recomputed since
    /// serialization) must not be paged out against the stale bytes.
    #[test]
    fn evict_keys_skips_nodes_holding_a_different_value() {
        let mut core = CoreState::new();
        let v = VersionNumber::FIRST;
        let (epoch, _ctx) = core.ctx_at_version(v);
        let key = DiceKey { index: 0 };
        let value = DiceValidValue::testing_new(DiceKeyValue::<K>::new(1));
        core.update_computed(
            VersionedGraphKey::new(v, key),
            epoch,
            StorageType::Normal,
            value.dupe(),
            ValueReusable::EqualityBased,
            Arc::new(SeriesParallelDeps::None),
            TrackedInvalidationPaths::clean(),
        )
        .unpack()
        .unwrap();

        // Same contents, different allocation - models a recompute that landed
        // between serialization and the evict message.
        let stale = DiceValidValue::testing_new(DiceKeyValue::<K>::new(1));
        core.evict_keys(vec![(key, pagable::DataKey::testing_new(1), stale)]);
        let status = core.pagable_status();
        assert_eq!(
            (status.resident.len(), status.paged_out.len()),
            (1, 0),
            "stale-value eviction must be skipped"
        );

        core.evict_keys(vec![(key, pagable::DataKey::testing_new(1), value)]);
        let status = core.pagable_status();
        assert_eq!(
            (status.resident.len(), status.paged_out.len()),
            (0, 1),
            "matching-value eviction pages out"
        );
    }

    /// Pressure candidates exclude values referenced by an active
    /// transaction's cache and values whose rdep has a pending task.
    #[tokio::test]
    async fn pressure_candidates_exclude_pinned_and_pending_rdeps() {
        let mut core = CoreState::new();
        let v = VersionNumber::FIRST;
        let (epoch, cache) = core.ctx_at_version(v);
        let dep = DiceKey { index: 1 };
        let parent = DiceKey { index: 2 };
        core.update_computed(
            VersionedGraphKey::new(v, dep),
            epoch,
            StorageType::Normal,
            DiceValidValue::testing_new(DiceKeyValue::<K>::new(1)),
            ValueReusable::EqualityBased,
            Arc::new(SeriesParallelDeps::None),
            TrackedInvalidationPaths::clean(),
        )
        .unpack()
        .unwrap();
        // Records the rdep edge dep -> parent.
        core.update_computed(
            VersionedGraphKey::new(v, parent),
            epoch,
            StorageType::Normal,
            DiceValidValue::testing_new(DiceKeyValue::<K>::new(2)),
            ValueReusable::EqualityBased,
            Arc::new(SeriesParallelDeps::serial_from_vec(vec![dep])),
            TrackedInvalidationPaths::clean(),
        )
        .unpack()
        .unwrap();

        // A pending task for `parent` in the active cache: `parent` is
        // referenced, and `dep` has a pending rdep - both must be excluded.
        let pending_task = spawn_dice_task(parent, &TokioSpawner, &(), |handle| {
            async move {
                let _handle = handle;
                futures::future::pending().await
            }
            .boxed()
        });
        cache.testing_insert_task(parent, pending_task);

        // Mirror production: sets built off-thread from the active caches.
        let collect = |core: &CoreState| {
            let mut referenced = crate::HashSet::default();
            let mut pending = crate::HashSet::default();
            for c in core.active_caches() {
                c.collect_referenced_keys(&mut referenced, &mut pending);
            }
            (referenced, pending)
        };

        let (referenced, pending) = collect(&core);
        assert!(
            core.pressure_eviction_candidates(&referenced, &pending)
                .is_empty(),
            "pinned value and pending-rdep dep must both be excluded"
        );

        // Transaction gone: both become candidates.
        core.drop_ctx_at_version(v);
        let (referenced, pending) = collect(&core);
        let mut keys: Vec<_> = core
            .pressure_eviction_candidates(&referenced, &pending)
            .iter()
            .map(|c| c.key.index)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2]);
    }

    #[test]
    fn update_state_gets_next_version() {
        let mut core = CoreState::new();

        assert_eq!(
            core.update_state([(
                DiceKey { index: 0 },
                ChangeType::Invalidate,
                InvalidationSourcePriority::Normal
            )]),
            VersionNumber::new(2)
        );

        assert_eq!(
            core.update_state([(
                DiceKey { index: 1 },
                ChangeType::Invalidate,
                InvalidationSourcePriority::Normal
            )]),
            VersionNumber::new(3)
        );
    }

    #[test]
    fn state_ctx_at_version() {
        let mut core = CoreState::new();
        let v = VersionNumber::new(1);

        let (epoch, ctx) = core.ctx_at_version(v);

        let (epoch1, ctx1) = core.ctx_at_version(v);
        assert!(ctx.ptr_eq(&ctx1));
        assert_eq!(epoch, epoch1);

        // if you drop one, there is still reference so getting the same version should give the
        // same instance of ctx
        core.drop_ctx_at_version(v);
        let (epoch2, ctx2) = core.ctx_at_version(v);
        assert!(ctx.ptr_eq(&ctx2));
        assert_eq!(epoch1, epoch2);

        // drop all references, should give a different ctx instance
        core.drop_ctx_at_version(v);
        core.drop_ctx_at_version(v);
        let (another_epoch, another) = core.ctx_at_version(v);
        assert!(!ctx.ptr_eq(&another));
        assert_ne!(another_epoch, epoch);
    }

    #[test]
    fn non_pageable_nodes_are_not_page_out_candidates() {
        let mut core = CoreState::new();
        let v = VersionNumber::FIRST;
        let (epoch, _ctx) = core.ctx_at_version(v);

        let compute = |core: &mut CoreState, index: u32| {
            let res = core.update_computed(
                VersionedGraphKey::new(v, DiceKey { index }),
                epoch,
                StorageType::Normal,
                DiceValidValue::testing_new(DiceKeyValue::<K>::new(index as usize)),
                ValueReusable::EqualityBased,
                Arc::new(SeriesParallelDeps::None),
                TrackedInvalidationPaths::clean(),
            );
            assert!(res.unpack().is_ok());
        };
        compute(&mut core, 0);
        compute(&mut core, 1);

        let candidates = |core: &CoreState| {
            let mut keys: Vec<u32> = core
                .keys_to_page_out()
                .into_iter()
                .map(|(k, _)| k.index)
                .collect();
            keys.sort();
            keys
        };

        // Both freshly-computed resident values are page-out candidates.
        assert_eq!(candidates(&core), vec![0, 1]);

        // Marking one non-pageable (its value can't be serialized) drops it from
        // the candidate set, so page-out won't keep retrying it; the other is
        // unaffected.
        core.mark_non_pageable(vec![DiceKey { index: 0 }]);
        assert_eq!(candidates(&core), vec![1]);
    }

    async fn make_finished_cancelling_task(key: DiceKey) -> DiceTask {
        let finished_cancelling_tasks = spawn_dice_task(key, &TokioSpawner, &(), |handle| {
            async move {
                let _handle = handle;
                futures::future::pending().await
            }
            .boxed()
        });
        finished_cancelling_tasks
            .as_ref()
            .cancel(TransactionCancelled);

        finished_cancelling_tasks.as_ref().await_termination().await;

        finished_cancelling_tasks
    }

    struct BlockCancel(Arc<Semaphore>);

    impl Drop for BlockCancel {
        fn drop(&mut self) {
            self.0.add_permits(1)
        }
    }

    async fn make_yet_to_cancel_tasks(key: DiceKey) -> (DiceTask, BlockCancel, Arc<Semaphore>) {
        let block_cancel = Arc::new(Semaphore::new(0));
        let arrive_cancel = Arc::new(Semaphore::new(0));
        let block_cancel_task = block_cancel.dupe();
        let arrive_cancel_task = arrive_cancel.dupe();
        let yet_to_cancel_tasks = spawn_dice_task(key, &TokioSpawner, &(), move |handle| {
            let block_cancel = block_cancel_task.dupe();
            let arrive_cancel = arrive_cancel_task.dupe();
            async move {
                handle
                    .cancellation_ctx()
                    .critical_section(|| async move {
                        arrive_cancel.add_permits(1);
                        let _guard = block_cancel.acquire().await.unwrap();
                        arrive_cancel.add_permits(1);
                    })
                    .await;

                Box::new(()) as Box<dyn Any + Send>
            }
            .boxed()
        });
        arrive_cancel.acquire().await.unwrap().forget();

        (
            yet_to_cancel_tasks,
            BlockCancel(block_cancel),
            arrive_cancel,
        )
    }

    async fn make_never_cancellable_task(key: DiceKey) -> DiceTask {
        let arrive_never_cancel = Arc::new(Semaphore::new(0));
        let arrive_never_cancel_task = arrive_never_cancel.dupe();
        let never_cancel_tasks = spawn_dice_task(key, &TokioSpawner, &(), move |handle| {
            let arrive_never_cancel = arrive_never_cancel_task.dupe();
            async move {
                handle
                    .cancellation_ctx()
                    .critical_section(|| async move {
                        arrive_never_cancel.add_permits(1);
                        futures::future::pending().await
                    })
                    .await
            }
            .boxed()
        });

        arrive_never_cancel.acquire().await.unwrap().forget();

        never_cancel_tasks
    }

    #[tokio::test]
    async fn state_tracks_pending_cancellation() {
        let mut core = CoreState::new();
        let v = VersionNumber::new(1);

        let (_epoch, cache) = core.ctx_at_version(v);

        let completed_key1 = DiceKey { index: 10 };
        let completed_key2 = DiceKey { index: 20 };
        let completed_task1 = make_completed_task::<K>(completed_key1, 1);
        let completed_task2 = make_completed_task::<K>(completed_key2, 2);

        let finished_cancelling_key1 = DiceKey { index: 30 };
        let finished_cancelling_key2 = DiceKey { index: 40 };
        let finished_cancelling_tasks1 =
            make_finished_cancelling_task(finished_cancelling_key1).await;
        let finished_cancelling_tasks2 =
            make_finished_cancelling_task(finished_cancelling_key2).await;

        let pending_key1 = DiceKey { index: 50 };
        let pending_key2 = DiceKey { index: 60 };
        let (yet_to_cancel_tasks1, guard1, arrive_cancel1) =
            make_yet_to_cancel_tasks(pending_key1).await;
        let (yet_to_cancel_tasks2, guard2, arrive_cancel2) =
            make_yet_to_cancel_tasks(pending_key2).await;

        let never_cancel_key1 = DiceKey { index: 100500 };
        let never_cancel_tasks1 = make_never_cancellable_task(never_cancel_key1).await;

        cache.testing_insert_task(completed_key1, completed_task1);
        cache.testing_insert_task(completed_key2, completed_task2);
        cache.testing_insert_task(finished_cancelling_key1, finished_cancelling_tasks1);
        cache.testing_insert_task(finished_cancelling_key2, finished_cancelling_tasks2);
        cache.testing_insert_task(pending_key1, yet_to_cancel_tasks1);
        cache.testing_insert_task(pending_key2, yet_to_cancel_tasks2);
        cache.testing_insert_task(never_cancel_key1, never_cancel_tasks1);

        core.drop_ctx_at_version(v);

        assert_eq!(core.get_tasks_pending_cancellation().len(), 3);

        assert!(matches!(
            cache.insert(DiceKey { index: 999 },),
            SharedCacheInsert::TransactionCancelled(_)
        ));

        // let the cancellable tasks cancel
        drop(guard1);
        drop(guard2);

        // wait for the cancellable tasks to actually cancel
        let _p = arrive_cancel1.acquire().await.unwrap();
        let _p = arrive_cancel2.acquire().await.unwrap();

        let (_epoch, cache) = core.ctx_at_version(v);

        let never_cancel_tasks2 = make_never_cancellable_task(DiceKey { index: 300 }).await;

        cache.testing_insert_task(DiceKey { index: 300 }, never_cancel_tasks2);

        core.drop_ctx_at_version(v);

        assert_eq!(core.get_tasks_pending_cancellation().len(), 2);
    }

    #[derive(Allocative, Clone, Debug, Display, Eq, PartialEq, Hash, Pagable)]
    #[pagable_typetag(DiceKeyDyn)]
    struct K;

    #[async_trait]
    impl Key for K {
        type Value = usize;

        async fn compute(
            &self,
            _ctx: &mut DiceComputations,
            _cancellations: &CancellationContext,
        ) -> Self::Value {
            unimplemented!("test")
        }

        fn equality(_: &Self::Value, _: &Self::Value) -> bool {
            true
        }

        fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
            NoValueSerialize::<Self::Value>::new()
        }
    }
}
