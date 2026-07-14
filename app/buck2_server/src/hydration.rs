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

use async_trait::async_trait;
use buck2_cli_proto::HydrationSubcommand;
use buck2_common::memory;
use buck2_server_ctx::ctx::ServerCommandContextTrait;
use buck2_server_ctx::partial_result_dispatcher::NoPartialResult;
use buck2_server_ctx::partial_result_dispatcher::PartialResultDispatcher;
use buck2_server_ctx::template::ServerCommandTemplate;
use buck2_server_ctx::template::run_server_command;
use dice::Dice;
use dice::DiceTransaction;
use dice::PagableStatus;
use dupe::Dupe;

use crate::ctx::ServerCommandContext;

pub(crate) async fn hydration_command(
    ctx: &ServerCommandContext<'_>,
    partial_result_dispatcher: PartialResultDispatcher<NoPartialResult>,
    req: buck2_cli_proto::HydrationRequest,
) -> buck2_error::Result<buck2_cli_proto::HydrationResponse> {
    let dice = ctx.base_context.daemon.dice_manager.unsafe_dice().dupe();
    let subcommand = HydrationSubcommand::try_from(req.subcommand)?;
    run_server_command(
        HydrationServerCommand { dice, subcommand },
        ctx,
        partial_result_dispatcher,
    )
    .await
}

struct HydrationServerCommand {
    dice: Arc<Dice>,
    subcommand: HydrationSubcommand,
}

#[async_trait]
impl ServerCommandTemplate for HydrationServerCommand {
    type StartEvent = buck2_data::HydrationCommandStart;
    type EndEvent = buck2_data::HydrationCommandEnd;
    type Response = buck2_cli_proto::HydrationResponse;
    type PartialResult = NoPartialResult;

    fn exclusive_command_name(&self) -> Option<String> {
        match self.subcommand {
            HydrationSubcommand::PageOut | HydrationSubcommand::PageIn => {
                Some("hydration".to_owned())
            }
            // Read-only reports.
            HydrationSubcommand::Status | HydrationSubcommand::DupStrings => None,
        }
    }

    async fn command(
        &self,
        _server_ctx: &dyn ServerCommandContextTrait,
        _partial_result_dispatcher: PartialResultDispatcher<Self::PartialResult>,
        _ctx: DiceTransaction,
    ) -> buck2_error::Result<Self::Response> {
        match self.subcommand {
            HydrationSubcommand::PageOut => {
                self.dice.page_out().await.map_err(|e| {
                    buck2_error::conversion::from_any_with_tag(
                        e,
                        buck2_error::ErrorTag::Environment,
                    )
                })?;
                // metrics() blocks on the core-state thread, draining its FIFO
                // queue, so the page-out evictions are processed before we purge.
                let _ = self.dice.metrics();
                memory::purge_jemalloc()?;
                // Cross-restart persistence: page-out doubles as the snapshot
                // save point when a snapshot path is configured. The next
                // daemon (same env) loads it at construction.
                if let Some((meta_path, digest)) =
                    buck2_build_api::configure_dice::dice_snapshot_env_config()
                {
                    // Values that serialize but cannot survive hydration
                    // (frozen starlark modules carry skipped native-function
                    // slots that panic on deserialize) persist key-only.
                    let deny: std::collections::HashSet<String> =
                        std::env::var("BUCK2_DICE_SNAPSHOT_DENY")
                            .unwrap_or_else(|_| {
                                // EvalImportKey: frozen modules panic on
                                // hydrate (NativeFunc skip slots).
                                // BuildKey + ensure keys: their computed
                                // effect includes declaring outputs to the
                                // per-daemon materializer tree - serving
                                // them dice-clean in a fresh daemon leaves
                                // write artifacts undeclared, and the
                                // uploader misclassifies them as source
                                // files (the LPTF upload failures). Re-
                                // executing is cheap: real actions AC-hit.
                                "EvalImportKey,BuildKey,EnsureProjectedArtifactKey,EnsureTransitiveSetProjectionKey".to_owned()
                            })
                            .split(',')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.trim().to_owned())
                            .collect();
                    let stats = self
                        .dice
                        .save_persisted_snapshot(&meta_path, digest, &deny)
                        .await
                        .map_err(|e| {
                            buck2_error::conversion::from_any_with_tag(
                                e,
                                buck2_error::ErrorTag::Environment,
                            )
                        })?;
                    tracing::info!(
                        "dice snapshot saved: {} nodes, {} injected, {} key-only, {} dropped",
                        stats.nodes_persisted,
                        stats.nodes_injected,
                        stats.keys_only,
                        stats.dropped_unserializable,
                    );
                }
                Ok(buck2_cli_proto::HydrationResponse::default())
            }
            HydrationSubcommand::PageIn => {
                self.dice.page_in().await.map_err(|e| {
                    buck2_error::conversion::from_any_with_tag(
                        e,
                        buck2_error::ErrorTag::Environment,
                    )
                })?;
                Ok(buck2_cli_proto::HydrationResponse::default())
            }
            HydrationSubcommand::Status => {
                let status = self.dice.pagable_status().await;
                Ok(buck2_cli_proto::HydrationResponse {
                    summary: Some(format_status_summary(&status)),
                })
            }
            HydrationSubcommand::DupStrings => {
                // O(all frozen-heap strings), seconds on a GB-scale daemon;
                // run it on a blocking thread, not the command executor.
                let summary = tokio::task::spawn_blocking(|| {
                    let heaps = starlark::values::all_live_frozen_heaps();
                    format_dup_strings_summary(&starlark::values::dup_string_stats(&heaps, 20))
                })
                .await
                .map_err(|e| {
                    buck2_error::conversion::from_any_with_tag(e, buck2_error::ErrorTag::Tier0)
                })?;
                Ok(buck2_cli_proto::HydrationResponse {
                    summary: Some(summary),
                })
            }
        }
    }
}

/// Render `dup_string_stats` for `buck2 debug hydration dup-strings`. The
/// "duplicated" line is the freeze-time interning ceiling (L1 of the dice
/// tail-memory plan): bytes reclaimed if every duplicate collapsed to one
/// allocation. Payload bytes only - arena headers/alignment excluded, so the
/// real win is somewhat larger.
fn format_dup_strings_summary(stats: &starlark::values::DupStringStats) -> String {
    let dup_strings = stats.total_strings - stats.distinct_strings;
    let dup_bytes = stats.total_bytes - stats.distinct_bytes;
    let pct = if stats.total_bytes > 0 {
        (dup_bytes as f64) * 100.0 / (stats.total_bytes as f64)
    } else {
        0.0
    };
    let mut out = format!(
        "Frozen-heap strings: {} heaps, {} strings, {} payload bytes\n\
         distinct:   {} strings, {} bytes\n\
         duplicated: {} copies, {} bytes ({:.1}% - the interning ceiling)\n",
        stats.heaps,
        stats.total_strings,
        stats.total_bytes,
        stats.distinct_strings,
        stats.distinct_bytes,
        dup_strings,
        dup_bytes,
        pct,
    );
    if !stats.top.is_empty() {
        out.push_str(&format!(
            "\n{:>12}  {:>8}  {:>8}  string\n",
            "wasted", "copies", "len"
        ));
        for t in &stats.top {
            let sample: String = t.sample.chars().take(60).collect();
            out.push_str(&format!(
                "{:>12}  {:>8}  {:>8}  {:?}\n",
                t.wasted_bytes(),
                t.copies,
                t.len,
                sample,
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use starlark::values::DupStringStat;
    use starlark::values::DupStringStats;

    use super::format_dup_strings_summary;

    #[test]
    fn dup_strings_summary_renders_totals_and_top_table() {
        let stats = DupStringStats {
            heaps: 3,
            total_strings: 5,
            total_bytes: 73,
            distinct_strings: 3,
            distinct_bytes: 37,
            top: vec![DupStringStat {
                sample: "shared-flag-string".to_owned(),
                len: 18,
                copies: 3,
            }],
        };
        let s = format_dup_strings_summary(&stats);
        assert!(s.contains("3 heaps, 5 strings, 73 payload bytes"));
        assert!(s.contains("duplicated: 2 copies, 36 bytes (49.3%"));
        assert!(s.contains("\"shared-flag-string\""));
    }

    #[test]
    fn dup_strings_summary_empty_is_divide_by_zero_safe() {
        let s = format_dup_strings_summary(&DupStringStats::default());
        assert!(s.contains("(0.0%"));
    }
}

fn format_status_summary(status: &PagableStatus) -> String {
    // `total_nodes` counts vacant/in-progress nodes too; the rest is "other".
    // saturating_sub guards an underflow the struct invariant already rules out.
    let other = status
        .total_nodes
        .saturating_sub(status.resident_count)
        .saturating_sub(status.paged_out_count);
    let mut summary = format!(
        "DICE hydration: {} nodes ({} resident, {} paged out, {} other)\n",
        status.total_nodes, status.resident_count, status.paged_out_count, other,
    );
    if !status.by_type.is_empty() {
        summary.push('\n');
        summary.push_str(&format!(
            "{:>12}  {:>12}  {}\n",
            "resident", "paged-out", "key type"
        ));
        for t in &status.by_type {
            summary.push_str(&format!(
                "{:>12}  {:>12}  {}\n",
                t.resident, t.paged_out, t.key_type
            ));
        }
    }
    summary
}
