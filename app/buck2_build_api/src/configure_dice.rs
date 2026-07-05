/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use buck2_common::dice::cells::SetCellResolver;
use buck2_common::dice::data::SetIoProvider;
use buck2_common::io::IoProvider;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::dice::SetLegacyConfigs;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;
use buck2_common::tenting::SetTentingAclProvider;
use buck2_common::tenting::TentingAclProvider;
use buck2_core::rollout_percentage::RolloutPercentage;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::digest_config::SetDigestConfig;
use dice::DetectCycles;
use dice::Dice;
use dice::DiceStorage;

use crate::actions::execute::dice_data::SetInvalidationTrackingConfig;
use crate::build::detailed_aggregated_metrics::dice::SetDetailedAggregatedMetricsHandle;
use crate::build::detailed_aggregated_metrics::events::DetailedAggregatedMetricsHandle;

/// Cross-restart persistence knobs, shared by the load site (here) and the
/// save site (`buck2 debug hydration page-out`). Returns the snapshot
/// metadata path plus the inputs digest that gates reuse: blake3 of the
/// buck2 revision and the optional `BUCK2_DICE_SNAPSHOT_SEED` (CI sets the
/// seed to a hash of everything else it considers identity-defining, e.g.
/// toolchain pins and buckconfig).
pub fn dice_snapshot_env_config() -> Option<(std::path::PathBuf, [u8; 32])> {
    let path = std::env::var("BUCK2_DICE_SNAPSHOT_PATH").ok()?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(buck2_build_info::revision().unwrap_or("dev").as_bytes());
    hasher.update(b"\0");
    hasher.update(
        std::env::var("BUCK2_DICE_SNAPSHOT_SEED")
            .unwrap_or_default()
            .as_bytes(),
    );
    Some((
        std::path::PathBuf::from(path),
        *hasher.finalize().as_bytes(),
    ))
}

/// Utility to configure the dice globals.
/// One place to not forget to initialize something in all places.
pub async fn configure_dice_for_buck(
    io: Arc<dyn IoProvider>,
    digest_config: DigestConfig,
    root_config: Option<&LegacyBuckConfig>,
    detect_cycles: Option<DetectCycles>,
    tenting_acl_provider: Option<Arc<dyn TentingAclProvider>>,
) -> buck2_error::Result<Arc<Dice>> {
    let detect_cycles = detect_cycles.map_or_else(
        || {
            root_config
                .and_then(|c| {
                    c.parse::<DetectCycles>(BuckconfigKeyRef {
                        section: "buck2",
                        property: "detect_cycles",
                    })
                    .transpose()
                })
                .unwrap_or(Ok(DetectCycles::Enabled))
        },
        Ok,
    )?;

    let mut dice = Dice::builder();
    dice.set_io_provider(io);
    dice.set_digest_config(digest_config);
    dice.set_tenting_acl_provider(tenting_acl_provider);
    let invalidation_tracking_enabled = match root_config {
        Some(c) => c
            .parse::<RolloutPercentage>(BuckconfigKeyRef {
                section: "buck2",
                property: "invalidation_tracking_enabled",
            })?
            .is_some_and(|v| v.roll()),
        None => false,
    };
    dice.set_invalidation_tracking_config(invalidation_tracking_enabled);

    // Empty handle; a command enables the tracker lazily if it needs one.
    dice.set_detailed_aggregated_metrics_handle(DetailedAggregatedMetricsHandle::new());

    // Opt-in pagable storage. When `BUCK2_DICE_DB_PATH` is set, configures a
    // `DiceStorage` backend, configured by `PAGABLE_STORAGE_BACKEND` so `Dice::page_out()`
    // (e.g. via `buck2 debug hydration page-out`) can serialize node values to disk.
    if let Ok(path) = std::env::var("BUCK2_DICE_DB_PATH") {
        let storage = DiceStorage::open(std::path::Path::new(&path)).map_err(|e| {
            buck2_error::conversion::from_any_with_tag(e, buck2_error::ErrorTag::Environment)
        })?;
        dice.set_pagable_storage(storage);
    }

    let dice = dice.build(detect_cycles);

    // Opt-in cross-restart persistence: with both `BUCK2_DICE_DB_PATH` and
    // `BUCK2_DICE_SNAPSHOT_PATH` set, a graph snapshot saved by
    // `buck2 debug hydration page-out` is loaded into the fresh daemon.
    // Header mismatch or a missing file is a silent cold start.
    let loaded = match dice_snapshot_env_config() {
        Some((meta_path, digest)) => dice
            .load_persisted_snapshot(&meta_path, digest)
            .await
            .map_err(|e| {
                buck2_error::conversion::from_any_with_tag(e, buck2_error::ErrorTag::Environment)
            })?,
        None => None,
    };
    match loaded {
        Some(stats) => {
            // The none-seeding below is skipped: the injected keys exist with
            // their persisted values, and the first command's changed_to
            // calls diff live reality against that baseline. Seeding None in
            // between would spuriously dirty every cell-dependent node.
            tracing::info!(
                "dice snapshot loaded: {} nodes, {} injected, {} key-only, {} dropped",
                stats.nodes_persisted,
                stats.nodes_injected,
                stats.keys_only,
                stats.dropped_unserializable + stats.dropped_dangling_dep,
            );
        }
        None => {
            let mut dice_ctx = dice.updater();
            dice_ctx.set_none_cell_resolver()?;
            dice_ctx.set_none_legacy_config_external_data()?;
            dice_ctx.commit().await;
        }
    }

    Ok(dice)
}
