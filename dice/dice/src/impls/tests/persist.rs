/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! End-to-end tests for DICE graph snapshot persistence: save in one Dice,
//! load into a fresh one (simulating a new process), and verify that clean
//! subgraphs are reused and dirty frontiers recompute.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use allocative::Allocative;
use async_trait::async_trait;
use derive_more::Display;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;
use tempfile::tempdir;

use crate::DiceKeyDyn;
use crate::DiceStorage;
use crate::api::computations::DiceComputations;
use crate::api::cycles::DetectCycles;
use crate::api::injected::InjectedKey;
use crate::api::key::Key;
use crate::api::key::NoValueSerialize;
use crate::api::key::PagableValueSerialize;
use crate::api::key::ValueSerialize;
use crate::api::user_data::UserComputationData;
use crate::impls::dice::Dice;

#[derive(Clone, Dupe)]
struct ComputeCounter(Arc<AtomicUsize>);

impl ComputeCounter {
    fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// Injected leaf - the invalidation frontier.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[pagable_typetag(DiceKeyDyn)]
struct Leaf(u32);

impl InjectedKey for Leaf {
    type Value = u64;

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

/// Computed node depending on `Leaf(n)`.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[pagable_typetag(DiceKeyDyn)]
struct Derived(u32);

#[async_trait]
impl Key for Derived {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        if let Ok(c) = ctx.per_transaction_data().data.get::<ComputeCounter>() {
            c.0.fetch_add(1, Ordering::SeqCst);
        }
        ctx.compute(&Leaf(self.0)).await.unwrap() * 10
    }

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

/// A computed node whose value declines serialization - persisted as
/// key-only, must recompute after load without breaking anything else.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[pagable_typetag(DiceKeyDyn)]
struct Opaque(u32);

#[async_trait]
impl Key for Opaque {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        if let Ok(c) = ctx.per_transaction_data().data.get::<ComputeCounter>() {
            c.0.fetch_add(1, Ordering::SeqCst);
        }
        ctx.compute(&Leaf(self.0)).await.unwrap() + 1
    }

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::<Self::Value>::new()
    }
}

fn make_dice(path: &std::path::Path) -> anyhow::Result<Arc<Dice>> {
    let storage = DiceStorage::open(path)?;
    let mut builder = Dice::builder();
    builder.set_pagable_storage(storage);
    Ok(builder.build(DetectCycles::Disabled))
}

fn user_data_with_counter(counter: &ComputeCounter) -> UserComputationData {
    let mut d = UserComputationData::new();
    d.data.set(counter.dupe());
    d
}

const DIGEST: [u8; 32] = [7; 32];

/// The golden path: build, save, load into a "new process", re-inject the
/// same leaf values - the derived node must be served without recompute.
#[tokio::test]
async fn warm_start_reuses_clean_graph() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let meta = tmp.path().join("dice-graph.meta");

    let counter1 = ComputeCounter::new();
    {
        let dice = make_dice(tmp.path())?;
        let mut updater = dice.updater_with_data(user_data_with_counter(&counter1));
        updater.changed_to(vec![(Leaf(1), 5u64)])?;
        let mut tx = updater.commit().await;
        assert_eq!(tx.compute(&Derived(1)).await?, 50);
        assert_eq!(counter1.count(), 1);
        drop(tx);
        dice.wait_for_idle().await;
        let stats = dice.save_persisted_snapshot(&meta, DIGEST).await?;
        assert_eq!(stats.nodes_persisted, 1, "Derived(1)");
        assert_eq!(stats.nodes_injected, 1, "Leaf(1)");
    }

    // "New process": fresh Dice, fresh key index, same store directory.
    let counter2 = ComputeCounter::new();
    let dice = make_dice(tmp.path())?;
    let loaded = dice.load_persisted_snapshot(&meta, DIGEST).await?;
    let stats = loaded.expect("snapshot should load");
    assert_eq!(stats.nodes_persisted, 1);
    assert_eq!(stats.nodes_injected, 1);

    let mut updater = dice.updater_with_data(user_data_with_counter(&counter2));
    updater.changed_to(vec![(Leaf(1), 5u64)])?; // unchanged reality
    let mut tx = updater.commit().await;
    assert_eq!(tx.compute(&Derived(1)).await?, 50);
    assert_eq!(
        counter2.count(),
        0,
        "clean subgraph must be reused, not recomputed"
    );
    Ok(())
}

/// Partial invalidation across a restart: one changed leaf recomputes its
/// dependent; the sibling subtree unrelated to the change is reused.
#[tokio::test]
async fn changed_leaf_dirties_only_its_dependents() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let meta = tmp.path().join("dice-graph.meta");

    let counter1 = ComputeCounter::new();
    {
        let dice = make_dice(tmp.path())?;
        let mut updater = dice.updater_with_data(user_data_with_counter(&counter1));
        updater.changed_to(vec![(Leaf(1), 5u64), (Leaf(2), 7u64)])?;
        let mut tx = updater.commit().await;
        assert_eq!(tx.compute(&Derived(1)).await?, 50);
        assert_eq!(tx.compute(&Derived(2)).await?, 70);
        assert_eq!(counter1.count(), 2);
        drop(tx);
        dice.wait_for_idle().await;
        dice.save_persisted_snapshot(&meta, DIGEST).await?;
    }

    let counter2 = ComputeCounter::new();
    let dice = make_dice(tmp.path())?;
    dice.load_persisted_snapshot(&meta, DIGEST)
        .await?
        .expect("snapshot should load");

    let mut updater = dice.updater_with_data(user_data_with_counter(&counter2));
    // Leaf(1) changed; Leaf(2) is unchanged reality.
    updater.changed_to(vec![(Leaf(1), 6u64), (Leaf(2), 7u64)])?;
    let mut tx = updater.commit().await;
    assert_eq!(tx.compute(&Derived(1)).await?, 60, "sees the new leaf");
    assert_eq!(tx.compute(&Derived(2)).await?, 70);
    assert_eq!(
        counter2.count(),
        1,
        "only Derived(1) recomputes; Derived(2) rides the snapshot"
    );
    Ok(())
}

/// The determinism gate: save -> load -> save must produce byte-identical
/// metadata. Canonical record ordering + content-addressed blobs make the
/// file a pure function of the graph.
#[tokio::test]
async fn save_load_save_is_byte_identical() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let meta_a = tmp.path().join("a.meta");
    let meta_b = tmp.path().join("b.meta");

    {
        let dice = make_dice(tmp.path())?;
        let mut updater = dice.updater();
        updater.changed_to(vec![(Leaf(1), 5u64), (Leaf(2), 7u64)])?;
        let mut tx = updater.commit().await;
        let _ = tx.compute(&Derived(1)).await?;
        let _ = tx.compute(&Derived(2)).await?;
        drop(tx);
        dice.wait_for_idle().await;
        dice.save_persisted_snapshot(&meta_a, DIGEST).await?;
    }

    let dice = make_dice(tmp.path())?;
    dice.load_persisted_snapshot(&meta_a, DIGEST)
        .await?
        .expect("snapshot should load");
    dice.wait_for_idle().await;
    dice.save_persisted_snapshot(&meta_b, DIGEST).await?;

    assert_eq!(
        std::fs::read(&meta_a)?,
        std::fs::read(&meta_b)?,
        "save/load/save must be deterministic"
    );
    Ok(())
}

/// A mismatched inputs digest is a silent cold start, never an error.
#[tokio::test]
async fn header_mismatch_is_a_cold_start() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let meta = tmp.path().join("dice-graph.meta");

    {
        let dice = make_dice(tmp.path())?;
        let mut updater = dice.updater();
        updater.changed_to(vec![(Leaf(1), 5u64)])?;
        let mut tx = updater.commit().await;
        let _ = tx.compute(&Derived(1)).await?;
        drop(tx);
        dice.wait_for_idle().await;
        dice.save_persisted_snapshot(&meta, DIGEST).await?;
    }

    let dice = make_dice(tmp.path())?;
    let loaded = dice.load_persisted_snapshot(&meta, [9; 32]).await?;
    assert!(loaded.is_none(), "different inputs => cold start");

    // Missing file is also a clean cold start.
    let dice2 = make_dice(tmp.path())?;
    let missing = dice2
        .load_persisted_snapshot(&tmp.path().join("nope.meta"), DIGEST)
        .await?;
    assert!(missing.is_none());
    Ok(())
}

/// Values that decline serialization persist as key-only records: their
/// dependents survive, they recompute on demand, nothing else breaks.
#[tokio::test]
async fn unserializable_value_degrades_to_recompute() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let meta = tmp.path().join("dice-graph.meta");

    let counter1 = ComputeCounter::new();
    {
        let dice = make_dice(tmp.path())?;
        let mut updater = dice.updater_with_data(user_data_with_counter(&counter1));
        updater.changed_to(vec![(Leaf(1), 5u64)])?;
        let mut tx = updater.commit().await;
        assert_eq!(tx.compute(&Opaque(1)).await?, 6);
        assert_eq!(tx.compute(&Derived(1)).await?, 50);
        drop(tx);
        dice.wait_for_idle().await;
        let stats = dice.save_persisted_snapshot(&meta, DIGEST).await?;
        assert_eq!(stats.keys_only, 1, "Opaque(1) persists key-only");
        assert_eq!(stats.nodes_persisted, 1, "Derived(1)");
    }

    let counter2 = ComputeCounter::new();
    let dice = make_dice(tmp.path())?;
    dice.load_persisted_snapshot(&meta, DIGEST)
        .await?
        .expect("snapshot should load");

    let mut updater = dice.updater_with_data(user_data_with_counter(&counter2));
    updater.changed_to(vec![(Leaf(1), 5u64)])?;
    let mut tx = updater.commit().await;
    assert_eq!(tx.compute(&Opaque(1)).await?, 6, "recomputes correctly");
    assert_eq!(tx.compute(&Derived(1)).await?, 50, "still cached");
    assert_eq!(counter2.count(), 1, "only the opaque node recomputed");
    Ok(())
}
