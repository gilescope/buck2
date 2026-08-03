/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Tests for the fork's pressure-eviction path (see [`crate::pressure`]).
//!
//! Lives under `core` rather than beside the implementation so it can reach
//! `CoreState`'s `pub(super)` methods without widening any of them.

use allocative::Allocative;
use async_trait::async_trait;
use derive_more::Display;
use dice_futures::cancellation::CancellationContext;
use dice_futures::spawner::TokioSpawner;
use dupe::Dupe;
use futures::FutureExt;
use pagable::Pagable;
use pagable::pagable_typetag;

use crate::DiceKeyDyn;
use crate::api::computations::DiceComputations;
use crate::api::key::Key;
use crate::api::key::NoValueSerialize;
use crate::api::key::ValueSerialize;
use crate::api::storage_type::StorageType;
use crate::arc::Arc;
use crate::core::graph::storage::ValueReusable;
use crate::core::graph::types::VersionedGraphKey;
use crate::core::internals::CoreState;
use crate::deps::graph::SeriesParallelDeps;
use crate::epoch::task::spawn_dice_task;
use crate::key::DiceKey;
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
