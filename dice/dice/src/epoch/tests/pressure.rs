/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! End-to-end tests for [`Dice::evict_under_pressure`] - L0 of the
//! tail-memory plan. Split out of `page_out.rs` so upstream's test file
//! carries none of the fork's tests; the shared fixtures are imported from
//! there rather than duplicated.

use allocative::Allocative;
use async_trait::async_trait;
use derive_more::Display;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;
use tempfile::tempdir;

use super::page_out::ComputeCounter;
use super::page_out::PagableKey;
use super::page_out::make_dice;
use super::page_out::user_data_with_counter;
use crate::DiceKeyDyn;
use crate::DiceStorage;
use crate::PagableStorageBackend;
use crate::api::computations::DiceComputations;
use crate::api::cycles::DetectCycles;
use crate::api::key::Key;
use crate::api::key::PagableValueSerialize;
use crate::api::key::ValueSerialize;
use crate::dice::Dice;
/// Second pagable key type, for allowlist tests.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[pagable_typetag(DiceKeyDyn)]
struct PagableKeyB(u32);

#[async_trait]
impl Key for PagableKeyB {
    type Value = u64;

    async fn compute(
        &self,
        _ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        u64::from(self.0) * 11
    }

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

fn allow(types: &[&str]) -> std::collections::HashSet<String> {
    types.iter().map(|s| (*s).to_owned()).collect()
}

/// Cold values are paged out under pressure and hydrate (not recompute) on the
/// next lookup.
#[tokio::test]
async fn evict_under_pressure_pages_out_cold_values() -> anyhow::Result<()> {
    let counter = ComputeCounter::new();
    let tmp = tempdir()?;
    let storage = DiceStorage::open(tmp.path(), PagableStorageBackend::Sqlite)?;
    let dice = make_dice(storage);

    let tx = dice
        .updater_with_data(user_data_with_counter(&counter))
        .commit()
        .await;
    let _: u64 = *tx.compute(&PagableKey(1)).await?;
    let _: u64 = *tx.compute(&PagableKey(2)).await?;
    drop(tx);

    let stats = dice
        .evict_under_pressure(usize::MAX, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(stats.candidates, 2);
    assert_eq!(stats.selected, 2);

    let status = dice.pagable_status().await;
    assert_eq!(status.paged_out_count, 2);

    let tx = dice
        .updater_with_data(user_data_with_counter(&counter))
        .commit()
        .await;
    let v: u64 = *tx.compute(&PagableKey(1)).await?;
    assert_eq!(v, 100);
    assert_eq!(counter.count(), 2, "lookup should hydrate, not recompute");

    Ok(())
}

/// Values referenced by a live transaction's cache are pinned (evicting them
/// frees nothing), so pressure eviction must skip them.
#[tokio::test]
async fn evict_under_pressure_skips_values_pinned_by_live_transaction() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let storage = DiceStorage::open(tmp.path(), PagableStorageBackend::Sqlite)?;
    let dice = make_dice(storage);

    let tx = dice.updater().commit().await;
    let _: u64 = *tx.compute(&PagableKey(1)).await?;

    // tx still alive: its cache references the value.
    let stats = dice
        .evict_under_pressure(usize::MAX, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(stats.candidates, 0, "pinned value must not be a candidate");
    let status = dice.pagable_status().await;
    assert_eq!(status.resident_count, 1);
    assert_eq!(status.paged_out_count, 0);

    drop(tx);

    let stats = dice
        .evict_under_pressure(usize::MAX, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(stats.selected, 1, "unpinned after the transaction dropped");
    let status = dice.pagable_status().await;
    assert_eq!(status.paged_out_count, 1);

    Ok(())
}

/// Only key types on the allowlist are evicted.
#[tokio::test]
async fn evict_under_pressure_respects_key_type_allowlist() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let storage = DiceStorage::open(tmp.path(), PagableStorageBackend::Sqlite)?;
    let dice = make_dice(storage);

    let tx = dice.updater().commit().await;
    let _: u64 = *tx.compute(&PagableKey(1)).await?;
    let _: u64 = *tx.compute(&PagableKeyB(1)).await?;
    drop(tx);

    let stats = dice
        .evict_under_pressure(usize::MAX, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(stats.candidates, 1);
    assert_eq!(stats.selected, 1);

    let status = dice.pagable_status().await;
    let paged = |name: &str| {
        status
            .by_type
            .iter()
            .find(|t| t.key_type == name)
            .map_or(0, |t| t.paged_out)
    };
    assert_eq!(paged("PagableKey"), 1);
    assert_eq!(paged("PagableKeyB"), 0, "not on the allowlist");

    Ok(())
}

/// `max_values` cuts selection off: a chunk of 1 evicts one value, not all.
#[tokio::test]
async fn evict_under_pressure_max_values_bounds_selection() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let storage = DiceStorage::open(tmp.path(), PagableStorageBackend::Sqlite)?;
    let dice = make_dice(storage);

    let tx = dice.updater().commit().await;
    for i in 0..4 {
        let _: u64 = *tx.compute(&PagableKey(i)).await?;
    }
    drop(tx);

    let stats = dice
        .evict_under_pressure(1, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(stats.candidates, 4);
    assert_eq!(stats.selected, 1, "chunk of 1 selects a single value");
    assert_eq!(dice.pagable_status().await.paged_out_count, 1);

    Ok(())
}

/// A value paged back in becomes `Recomputed`, which is not a page-out
/// candidate: a value is paged out at most once (see
/// `PagableNodeValue::after_recompute`), so pressure eviction leaves it resident
/// rather than paying to serialize it a second time.
#[tokio::test]
async fn evict_under_pressure_skips_a_value_paged_in_again() -> anyhow::Result<()> {
    let counter = ComputeCounter::new();
    let tmp = tempdir()?;
    let storage = DiceStorage::open(tmp.path(), PagableStorageBackend::Sqlite)?;
    let dice = make_dice(storage);

    let tx = dice
        .updater_with_data(user_data_with_counter(&counter))
        .commit()
        .await;
    let _: u64 = *tx.compute(&PagableKey(1)).await?;
    drop(tx);
    dice.wait_for_idle().await;
    dice.page_out().await?;

    // Hydrate it back in.
    let tx = dice
        .updater_with_data(user_data_with_counter(&counter))
        .commit()
        .await;
    let _: u64 = *tx.compute(&PagableKey(1)).await?;
    drop(tx);
    assert_eq!(counter.count(), 1);

    let stats = dice
        .evict_under_pressure(usize::MAX, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(
        (stats.candidates, stats.selected),
        (0, 0),
        "a value that has been paged out once is never offered again"
    );
    assert_eq!(dice.pagable_status().await.paged_out_count, 0);

    Ok(())
}

/// No pagable storage configured → no-op with empty stats.
#[tokio::test]
async fn evict_under_pressure_without_storage_is_noop() -> anyhow::Result<()> {
    let dice = Dice::builder().build(DetectCycles::Disabled);
    let stats = dice
        .evict_under_pressure(usize::MAX, &allow(&["PagableKey"]))
        .await?;
    assert_eq!(stats.selected, 0);
    Ok(())
}
