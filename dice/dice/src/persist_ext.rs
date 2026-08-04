/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Graph snapshot save/load entry points on [`Dice`].
//!
//! Fork addition; see [`crate::persist`] for the codec and the driver. Kept
//! out of `dice.rs` so that upstream file carries none of it.

use std::sync::Arc as StdArc;

use dupe::Dupe;
use futures::Future;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

use crate::core::graph::nodes::VersionedGraphNode;
use crate::core::internals::CoreState;
use crate::core::state::CoreStateHandle;
use crate::core::state::StateRequest;
use crate::dice::Dice;
use crate::key::DiceKey;
use crate::persist::PersistNodeExtract;
use crate::versions::VersionNumber;

impl Dice {
    /// Persist the whole graph as a snapshot: values into the configured
    /// `DiceStorage`, skeleton into `meta_path`. `inputs_digest` should hash
    /// everything identity-defining (binary, configs) - load validates it.
    ///
    /// Caller must ensure DICE is idle (`wait_for_idle().await`).
    pub async fn save_persisted_snapshot(
        self: &StdArc<Self>,
        meta_path: &std::path::Path,
        inputs_digest: [u8; 32],
        deny_key_types: &std::collections::HashSet<String>,
    ) -> anyhow::Result<crate::persist::PersistStats> {
        self.page_out().await?;
        let Some(storage) = self.pagable_storage.as_ref() else {
            return Err(anyhow::anyhow!(
                "save_persisted_snapshot requires pagable storage (set_pagable_storage)"
            ));
        };
        crate::persist::save_snapshot(
            &self.state_handle,
            &self.key_index,
            storage,
            meta_path,
            inputs_digest,
            deny_key_types,
        )
        .await
    }

    /// Load a snapshot saved by `save_persisted_snapshot` into this
    /// freshly-built DICE. Returns `Ok(None)` = cold start (missing file or
    /// header mismatch). Must be called before any computation runs.
    pub async fn load_persisted_snapshot(
        self: &StdArc<Self>,
        meta_path: &std::path::Path,
        inputs_digest: [u8; 32],
    ) -> anyhow::Result<Option<crate::persist::PersistStats>> {
        let Some(storage) = self.pagable_storage.as_ref() else {
            return Err(anyhow::anyhow!(
                "load_persisted_snapshot requires pagable storage (set_pagable_storage)"
            ));
        };
        crate::persist::load_snapshot(
            &self.state_handle,
            &self.key_index,
            storage,
            meta_path,
            inputs_digest,
        )
        .await
    }
}

impl CoreState {
    /// Persist support: extract every node's snapshot metadata. Runs on the
    /// state thread; everything returned is plain data + Arc clones.
    pub(crate) fn persist_extract(
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
    pub(crate) fn persist_install(
        &mut self,
        nodes: Vec<(DiceKey, VersionedGraphNode)>,
        at_version: VersionNumber,
    ) {
        self.graph.install_persisted_nodes(nodes, at_version);
        self.version_tracker.fast_forward_for_persist(at_version);
    }
}

/// Snapshot save/load core-state requests, carried as one [`StateRequest`]
/// variant. See [`crate::pressure::PressureRequest`] for the same shape.
pub(crate) enum PersistRequest {
    /// Extract every node's metadata for a snapshot, plus the version it was
    /// taken at.
    Extract {
        resp: Sender<(VersionNumber, Vec<PersistNodeExtract>)>,
    },
    /// Install reconstructed nodes into an empty graph and fast-forward the
    /// version counter to the snapshot's version.
    Install {
        nodes: Vec<(DiceKey, VersionedGraphNode)>,
        at_version: VersionNumber,
        resp: Sender<()>,
    },
}

impl PersistRequest {
    /// Run on the core-state thread. Dropped senders mean the caller went away.
    pub(crate) fn handle(self, state: &mut CoreState) {
        match self {
            PersistRequest::Extract { resp } => drop(resp.send(state.persist_extract())),
            PersistRequest::Install {
                nodes,
                at_version,
                resp,
            } => {
                state.persist_install(nodes, at_version);
                let _ignored = resp.send(());
            }
        }
    }
}

impl CoreStateHandle {
    /// Persist support: extract snapshot metadata for every node.
    pub(crate) fn persist_extract(
        &self,
    ) -> impl Future<Output = (VersionNumber, Vec<PersistNodeExtract>)> + use<> {
        let (resp, recv) = oneshot::channel();
        self.call(
            StateRequest::Persist(PersistRequest::Extract { resp }),
            recv,
        )
    }

    /// Persist support: install reconstructed nodes into the (empty) graph.
    pub(crate) fn persist_install(
        &self,
        nodes: Vec<(DiceKey, VersionedGraphNode)>,
        at_version: VersionNumber,
    ) -> impl Future<Output = ()> + use<> {
        let (resp, recv) = oneshot::channel();
        self.call(
            StateRequest::Persist(PersistRequest::Install {
                nodes,
                at_version,
                resp,
            }),
            recv,
        )
    }
}
