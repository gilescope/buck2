/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::path::Path;
use std::path::PathBuf;
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
use dice::PagableStorageBackend;

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
    // Path to open pagable DICE storage at, or `None` to leave paging disabled.
    dice_state_path: Option<&Path>,
    // On-disk backend for pagable storage (`buck2_hydration.pagable_storage_backend`).
    pagable_storage_backend: PagableStorageBackend,
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

    // Opt-in pagable storage, enabling `Dice::page_out()` to serialize node
    // values to disk (backend chosen by `pagable_storage_backend`) via `buck2
    // debug hydration`. `dice_state_path` is `Some` when
    // `buck2_hydration.enable_paging` is set (its value is the default path,
    // under buck-out). The `BUCK2_DICE_DB_PATH` override (used by benchmarks)
    // takes precedence and picks the path.
    let db_path: Option<PathBuf> = match std::env::var_os("BUCK2_DICE_DB_PATH") {
        Some(path) => Some(PathBuf::from(path)),
        None => dice_state_path.map(Path::to_path_buf),
    };
    if let Some(path) = db_path {
        let backend = pagable_storage_backend.with_env_override().map_err(|e| {
            buck2_error::conversion::from_any_with_tag(e, buck2_error::ErrorTag::Environment)
        })?;
        let storage = DiceStorage::open(&path, backend).map_err(|e| {
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

    // L0 watermark eviction: bound daemon RSS by paging out cold DICE values
    // mid-build. Opt-in via BUCK2_DICE_EVICT_HIGH (requires pagable storage).
    if std::env::var("BUCK2_DICE_DB_PATH").is_ok()
        && let Some((high, low)) = dice_evict_watermarks_env()
    {
        spawn_pressure_evictor(Arc::downgrade(&dice), high, low);
    }

    Ok(dice)
}

/// Watermark-eviction knobs (L0 of the dice tail-memory plan).
/// `BUCK2_DICE_EVICT_HIGH` / `BUCK2_DICE_EVICT_LOW` take bytes with an
/// optional K/M/G suffix (e.g. `6G`). LOW defaults to 75% of HIGH. Absolute
/// bytes, not fractions of RAM: the launcher (CI driver) knows the box.
fn dice_evict_watermarks_env() -> Option<(u64, u64)> {
    let high = parse_byte_size(&std::env::var("BUCK2_DICE_EVICT_HIGH").ok()?)?;
    let low = std::env::var("BUCK2_DICE_EVICT_LOW")
        .ok()
        .and_then(|v| parse_byte_size(&v))
        .unwrap_or(high / 4 * 3);
    if low >= high {
        tracing::warn!("BUCK2_DICE_EVICT_LOW >= BUCK2_DICE_EVICT_HIGH; eviction disabled");
        return None;
    }
    Some((high, low))
}

fn parse_byte_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = match s.as_bytes().last()? {
        b'K' | b'k' => (&s[..s.len() - 1], 1u64 << 10),
        b'M' | b'm' => (&s[..s.len() - 1], 1 << 20),
        b'G' | b'g' => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    num.trim().parse::<u64>().ok()?.checked_mul(mult)
}

/// Poll RSS; above `high`, evict cold values chunk by chunk until below `low`
/// (hysteresis - the gap prevents trigger/evict thrash at the boundary).
/// Eviction is count-paced with RSS as the controller's ground truth: per-value
/// resident size is not reliably measurable (values share arenas), so each
/// round evicts a chunk, purges the allocator, and re-measures.
fn spawn_pressure_evictor(dice: std::sync::Weak<Dice>, high: u64, low: u64) {
    let poll = std::time::Duration::from_secs(env_u64("BUCK2_DICE_EVICT_POLL_SECS", 10));
    let chunk = usize::try_from(env_u64("BUCK2_DICE_EVICT_CHUNK", 4096)).unwrap_or(4096);
    // AnalysisKey values dominate the build-tail heap (see
    // dice/docs/tail_memory_plan.md); other key types opt in via env.
    let allowed: std::collections::HashSet<String> = std::env::var("BUCK2_DICE_EVICT_ALLOW")
        .unwrap_or_else(|_| "AnalysisKey".to_owned())
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_owned())
        .collect();
    tracing::info!(
        "dice pressure evictor armed: high {high}B, low {low}B, poll {}s, chunk {chunk}, allow {allowed:?}",
        poll.as_secs()
    );
    tokio::spawn(async move {
        // Back-off multiplier: above the watermark with NOTHING evictable
        // (everything pinned by the running command), each poll still costs a
        // graph walk on the core-state thread. Polling that state at full
        // cadence measurably slowed an analysis-heavy build; nothing becomes
        // evictable until a transaction drops, so ease off until one does.
        let mut idle_backoff: u32 = 1;
        loop {
            tokio::time::sleep(poll * idle_backoff).await;
            let Some(dice) = dice.upgrade() else { return };
            let Some(mut rss) = buck2_util::process_stats::process_stats().rss_bytes else {
                continue;
            };
            if rss <= high {
                idle_backoff = 1;
                continue;
            }
            tracing::warn!(
                "dice pressure: rss {rss}B > high watermark {high}B; evicting to {low}B"
            );
            loop {
                let stats = match dice.evict_under_pressure(chunk, &allowed).await {
                    Ok(stats) => stats,
                    Err(e) => {
                        tracing::warn!("dice pressure eviction failed: {e:#}");
                        break;
                    }
                };
                let _ = buck2_common::memory::purge_jemalloc();
                let new_rss = buck2_util::process_stats::process_stats()
                    .rss_bytes
                    .unwrap_or(rss);
                tracing::info!(
                    "dice pressure: evicted {} of {} candidates ({} pre-serialized); rss {rss}B -> {new_rss}B",
                    stats.selected,
                    stats.candidates,
                    stats.already_serialized,
                );
                // Stop on target reached, candidates exhausted, or no forward
                // progress (evictions pinned elsewhere / allocator holding).
                if stats.selected == 0 {
                    idle_backoff = (idle_backoff * 2).min(8);
                    break;
                }
                idle_backoff = 1;
                if new_rss <= low || new_rss >= rss {
                    break;
                }
                rss = new_rss;
            }
        }
    });
}

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::parse_byte_size;

    #[test]
    fn parse_byte_size_suffixes() {
        assert_eq!(parse_byte_size("1024"), Some(1024));
        assert_eq!(parse_byte_size("4K"), Some(4096));
        assert_eq!(parse_byte_size("2m"), Some(2 << 20));
        assert_eq!(parse_byte_size("6G"), Some(6 << 30));
        assert_eq!(parse_byte_size(" 6 G "), Some(6 << 30));
        assert_eq!(parse_byte_size(""), None);
        assert_eq!(parse_byte_size("G"), None);
        assert_eq!(parse_byte_size("nope"), None);
        // Overflow must not wrap.
        assert_eq!(parse_byte_size("999999999999999999G"), None);
    }
}
