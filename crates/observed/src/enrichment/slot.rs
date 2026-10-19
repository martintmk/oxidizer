// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Thread-local enrichment storage: linked-list nodes, RAII guards, and cross-thread transfer.
//!
//! See the [Enrichment](crate#enrichment) section for a high-level overview.

use std::any::type_name;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use performables::arc::Arc;
use smallvec::{SmallVec, smallvec};
use thread_aware::{Thread, ThreadAware};

use super::enrichment_trait::collect_entries;
use crate::SinkId;
use crate::enrichment::{Enrichment, EnrichmentEntry};

pub(crate) type OptEnrichmentNode = Option<Arc<EnrichmentNode>>;

/// Inline capacity for per-sink enrichment slot lists. Composite sinks
/// typically fan out to only a handful of children, so 3 covers the common
/// case without heap-spilling; larger fan-outs spill to the heap transparently.
const INLINE_SLOT_CAPACITY: usize = 3;

const INLINE_ACTIVE_SLOT_CAPACITY: usize = 4;

thread_local! {
    // A shared inline list avoids a ThreadLocal allocation and its buckets per sink.
    // Only active heads are retained; a HashMap would allocate even for one sink.
    static HEADS: RefCell<SmallVec<[(usize, Arc<EnrichmentNode>); INLINE_ACTIVE_SLOT_CAPACITY]>> =
        const { RefCell::new(SmallVec::new_const()) };
}

/// A single node in the linked enrichment chain.
#[derive(Debug, Clone)]
pub(crate) struct EnrichmentNode {
    pub(crate) entries: Arc<[EnrichmentEntry]>,
    pub(crate) parent: OptEnrichmentNode,
}

/// Thread-local slot per sink, holding the current enrichment chain tail.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Slot(usize);

impl thread_aware::ThreadAware for Slot {
    fn relocate(&mut self, _source: Option<&Thread>, _destination: &Thread) {
        // Enrichment slot is thread local, it doesn't need to be relocated
    }
}

impl Slot {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let mut id = NEXT_ID.load(Ordering::Relaxed);
        loop {
            let next = id
                .checked_add(1)
                .expect("creating more than usize::MAX slots exhausts their unique identities");
            match NEXT_ID.compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Self(id),
                Err(current) => id = current,
            }
        }
    }

    pub(crate) fn current(&self) -> OptEnrichmentNode {
        HEADS.with(|heads| {
            heads
                .borrow()
                .iter()
                .find(|(id, _)| *id == self.0)
                .map(|(_, head)| Arc::clone(head))
        })
    }

    fn replace(&self, node: OptEnrichmentNode) -> OptEnrichmentNode {
        HEADS.with(|heads| {
            let mut heads = heads.borrow_mut();
            if let Some(index) = heads.iter().position(|(id, _)| *id == self.0) {
                match node {
                    Some(node) => Some(std::mem::replace(&mut heads[index].1, node)),
                    None => Some(heads.swap_remove(index).1),
                }
            } else {
                if let Some(node) = node {
                    heads.push((self.0, node));
                }
                None
            }
        })
    }

    /// Pushes entries onto the enrichment chain and returns a guard.
    ///
    /// An empty layer installs no chain node and records no restoration, so the
    /// storage boundary holds for callers that reach it without passing through
    /// [`Sink::push_enrichment`](crate::Sink).
    pub(crate) fn push(&self, entries: Arc<[EnrichmentEntry]>) -> Guard {
        if entries.is_empty() {
            return Guard::empty();
        }

        let prev = HEADS.with(|heads| {
            let mut heads = heads.borrow_mut();
            let index = heads.iter().position(|(id, _)| *id == self.0);
            let next = Arc::new(EnrichmentNode {
                entries,
                parent: index.map(|index| Arc::clone(&heads[index].1)),
            });
            if let Some(index) = index {
                Some(std::mem::replace(&mut heads[index].1, next))
            } else {
                heads.push((self.0, next));
                None
            }
        });
        Guard {
            slots: smallvec![(self.clone(), prev)],
        }
    }
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

/// RAII guard that restores previous enrichment heads when dropped.
///
/// Holds one `(slot, prev)` pair per mutation. Drop unwinds them in reverse
/// mutation order, so nested pushes onto the same slot restore correctly even
/// if a caller manages to record two mutations of one slot in a single guard.
#[must_use]
pub(crate) struct Guard {
    slots: SmallVec<[(Slot, OptEnrichmentNode); INLINE_SLOT_CAPACITY]>,
}

impl Guard {
    pub(crate) fn empty() -> Self {
        Self { slots: SmallVec::new() }
    }

    /// Flattens several guards into one. Each input guard is consumed and its
    /// `Drop` is disarmed (`mem::take` empties its slots), so the merged guard
    /// owns the restoration responsibility.
    pub(crate) fn merge(guards: impl IntoIterator<Item = Self>) -> Self {
        let mut slots = SmallVec::new();
        for mut g in guards {
            slots.extend(std::mem::take(&mut g.slots));
        }
        Self { slots }
    }
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        // Reverse order: the last mutation must be undone first, otherwise two
        // mutations of one slot would restore the older head and leave the
        // newer one installed.
        for (slot, prev) in self.slots.drain(..).rev() {
            let _ = slot.replace(prev);
        }
    }
}

/// Restores slot heads installed by an [`EnrichmentTransfer`].
///
/// The transfer already owns every captured [`Slot`], so this guard borrows
/// those handles instead of cloning their reference counts on every apply.
#[must_use]
pub(crate) struct AppliedGuard<'a> {
    slots: SmallVec<[(&'a Slot, OptEnrichmentNode); INLINE_SLOT_CAPACITY]>,
}

impl std::fmt::Debug for AppliedGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

impl Drop for AppliedGuard<'_> {
    fn drop(&mut self) {
        for (slot, prev) in self.slots.drain(..).rev() {
            let _ = slot.replace(prev);
        }
    }
}

/// Captured enrichment state for cross-thread transfer.
///
/// Created by [`Sink::transfer_context`](crate::Sink::transfer_context).
/// Carries one `(slot, head)` pair per leaf sink that contributed to the
/// capture - the unique identity inside [`Slot`]
/// is preserved across `Sink::clone`, so each pair addresses the *same*
/// enrichment chain on whichever thread the transfer is applied on.
///
/// Apply on the target thread via
/// [`Transfer::apply_current_thread`](crate::context::Transfer::apply_current_thread); each captured
/// slot's head is restored independently, so divergent state across
/// composite children round-trips correctly.
#[derive(Clone, Default, ThreadAware)]
pub(crate) struct EnrichmentTransfer {
    #[thread_aware(skip)] // immutable Arc-shared state; no locks involved
    slots: SmallVec<[(Slot, OptEnrichmentNode); INLINE_SLOT_CAPACITY]>,
}

impl EnrichmentTransfer {
    /// Appends a slot's current chain head to this transfer.
    pub(crate) fn add_slot(&mut self, slot: &Slot) {
        self.slots.push((slot.clone(), slot.current()));
    }

    /// Restores every captured `(slot, head)` on this thread, returning a
    /// guard that pops each slot back to its prior head on drop.
    ///
    /// Borrows `self` so the same transfer can be applied repeatedly (e.g.
    /// from a future's `poll` loop) without cloning the captured slots.
    pub(crate) fn apply(&self) -> AppliedGuard<'_> {
        let mut slots = SmallVec::with_capacity(self.slots.len());
        for (slot, head) in &self.slots {
            let prev = slot.replace(head.clone());
            slots.push((slot, prev));
        }
        AppliedGuard { slots }
    }

    /// Restores captured heads with an owned guard that can outlive this transfer.
    pub(crate) fn apply_owned(&self) -> Guard {
        let mut slots = SmallVec::with_capacity(self.slots.len());
        for (slot, head) in &self.slots {
            let prev = slot.replace(head.clone());
            slots.push((slot.clone(), prev));
        }
        Guard { slots }
    }

    /// Pushes an additional enrichment node onto every captured chain.
    /// Broadcast within the transfer's known slots; transfers with no
    /// captured slots are left unchanged.
    ///
    /// The iterator is checked before allocating its shared slice, so an empty
    /// layer skips the shared-slice allocation as well as the chain nodes.
    /// [`push_entries`](Self::push_entries) relies on this and on the same
    /// check in [`push_for`](Self::push_for) for its non-empty input.
    pub(crate) fn push(&mut self, additional_enrichment: impl Enrichment) {
        if self.slots.is_empty() {
            return;
        }

        let Some(entries) = collect_entries(additional_enrichment.into_entries()) else {
            return;
        };

        self.push_entries(&entries);
    }

    /// Same as [`push`](Self::push), but marks every entry as targeted at
    /// `target`, so only that sink observes it. Mirrors the `with_target`
    /// mapping that `EnrichFutureExt::enrich_for` applies.
    pub(crate) fn push_for(&mut self, target: SinkId, additional_enrichment: impl Enrichment) {
        if self.slots.is_empty() {
            return;
        }

        let Some(entries) = collect_entries(additional_enrichment.into_entries().map(|entry| entry.with_target(target))) else {
            return;
        };

        self.push_entries(&entries);
    }

    /// Layers `entries` onto every captured chain as a single new node.
    fn push_entries(&mut self, entries: &Arc<[EnrichmentEntry]>) {
        if self.slots.is_empty() {
            return;
        }

        for (_, node) in &mut self.slots {
            *node = Some(Arc::new(EnrichmentNode {
                entries: Arc::clone(entries),
                parent: node.take(),
            }));
        }
    }
}

impl std::fmt::Debug for EnrichmentTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod coverage_tests {
    use super::*;

    struct TestEnrichment(&'static str);

    impl Enrichment for TestEnrichment {
        fn into_entries(self) -> impl Iterator<Item = EnrichmentEntry> {
            [EnrichmentEntry::unclassified(self.0, 1_i64)].into_iter()
        }
    }

    struct EmptyEnrichment;

    impl Enrichment for EmptyEnrichment {
        fn into_entries(self) -> impl Iterator<Item = EnrichmentEntry> {
            std::iter::empty()
        }
    }

    struct UnreachableEnrichment;

    impl Enrichment for UnreachableEnrichment {
        fn into_entries(self) -> impl Iterator<Item = EnrichmentEntry> {
            fn fail() -> std::iter::Empty<EnrichmentEntry> {
                panic!("an empty transfer must not materialize enrichment entries")
            }
            fail()
        }
    }

    /// Flattens a chain into its entries, innermost node first.
    fn chain_entries(head: &OptEnrichmentNode) -> Vec<EnrichmentEntry> {
        let mut entries = Vec::new();
        let mut cursor = head.as_ref();
        while let Some(node) = cursor {
            entries.extend(node.entries.iter().cloned());
            cursor = node.parent.as_ref();
        }
        entries
    }

    #[test]
    fn clones_identify_the_same_slot() {
        // Slot identity is what lets a transfer address the same chain after a
        // `Sink::clone`, so a clone must compare equal and a fresh slot must not.
        let slot = Slot::new();

        assert_eq!(slot, slot.clone());
        assert_ne!(slot, Slot::new());
    }

    #[test]
    fn guard_restores_the_previous_head_on_drop() {
        let slot = Slot::new();
        assert!(slot.current().is_none());

        {
            let _guard = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("k", 1_i64)]));
            assert!(slot.current().is_some());
        }

        assert!(slot.current().is_none(), "dropping the guard must pop the pushed node");
    }

    #[test]
    fn empty_push_returns_noop_guard_without_touching_the_slot() {
        let slot = Slot::new();
        let empty_guard = slot.push(Arc::from(Vec::new()));
        assert!(slot.current().is_none());

        let real_guard = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("k", 1_i64)]));
        let merged = Guard::merge([empty_guard, real_guard]);
        assert!(slot.current().is_some());

        drop(merged);
        assert!(slot.current().is_none());
    }

    #[test]
    fn transfer_captures_a_slot_and_layers_further_enrichment_onto_it() {
        let slot = Slot::new();
        let mut transfer = EnrichmentTransfer::default();
        transfer.add_slot(&slot);

        transfer.push(TestEnrichment("global"));
        transfer.push_for(SinkId::new("target"), TestEnrichment("targeted"));

        let guard = transfer.apply();
        let entries = chain_entries(&slot.current());

        assert_eq!(entries.len(), 2, "both pushes must reach the captured slot");
        assert!(
            entries.iter().any(|entry| entry.target().is_none()),
            "`push` must add an untargeted entry"
        );
        assert!(
            entries.iter().any(|entry| entry.target() == Some(SinkId::new("target"))),
            "`push_for` must mark its entry as targeted"
        );

        drop(guard);
        assert!(slot.current().is_none());
    }

    static_assertions::assert_impl_all!(Slot: thread_aware::ThreadAware);

    #[test]
    fn relocation_preserves_thread_local_storage_identity() {
        let slot = Slot::new();
        let mut relocated = slot.clone();
        let destination = thread_aware::ThreadBuilder::default().build(std::thread::current().id());

        relocated.relocate(None, &destination);

        assert_eq!(slot, relocated);
    }

    #[test]
    fn inactive_slots_are_not_retained_in_thread_local_storage() {
        let slot = Slot::new();
        assert!(slot.replace(None).is_none());
        {
            let _guard = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("k", 1_i64)]));
            HEADS.with(|heads| assert!(heads.borrow().iter().any(|(id, _)| *id == slot.0)));
        }
        HEADS.with(|heads| assert!(heads.borrow().iter().all(|(id, _)| *id != slot.0)));
    }

    #[test]
    fn empty_capture_masks_and_restores_an_active_head() {
        let slot = Slot::new();
        let mut transfer = EnrichmentTransfer::default();
        transfer.add_slot(&slot);
        let _guard = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("previous", 1_i64)]));

        {
            let _applied = transfer.apply();
            assert!(slot.current().is_none());
        }

        assert_eq!(chain_entries(&slot.current())[0].key().as_str(), "previous");
    }

    #[test]
    fn merged_guard_restores_repeated_mutations_of_the_same_slot() {
        let slot = Slot::new();
        let first = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("first", 1_i64)]));
        let second = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("second", 2_i64)]));

        drop(Guard::merge([first, second]));

        assert!(slot.current().is_none());
    }

    #[test]
    fn transfer_is_thread_local_and_restores_destination_context() {
        let slot = Slot::new();
        let _source = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("source", 1_i64)]));
        let mut transfer = EnrichmentTransfer::default();
        transfer.add_slot(&slot);

        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert!(slot.current().is_none());
                    let _destination = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("destination", 2_i64)]));
                    {
                        let _applied = transfer.apply();
                        assert_eq!(chain_entries(&slot.current())[0].key().as_str(), "source");
                    }
                    assert_eq!(chain_entries(&slot.current())[0].key().as_str(), "destination");
                })
                .join()
                .unwrap();
        });

        assert_eq!(chain_entries(&slot.current())[0].key().as_str(), "source");
    }

    #[test]
    fn transfer_ignores_empty_enrichment_layers() {
        let slot = Slot::new();
        let mut transfer = EnrichmentTransfer::default();
        transfer.add_slot(&slot);

        transfer.push(EmptyEnrichment);
        transfer.push_for(SinkId::new("target"), EmptyEnrichment);

        let guard = transfer.apply();
        assert!(slot.current().is_none());

        drop(guard);
        assert!(slot.current().is_none());
    }

    #[test]
    fn applied_guard_restores_spilled_slot_storage() {
        const SLOT_COUNT: usize = INLINE_SLOT_CAPACITY + 1;

        let slots: [_; SLOT_COUNT] = std::array::from_fn(|_| Slot::new());
        let captured_guards: Vec<_> = slots
            .iter()
            .enumerate()
            .map(|(index, slot)| {
                slot.push(Arc::from(vec![EnrichmentEntry::unclassified(
                    "captured",
                    i64::try_from(index).unwrap(),
                )]))
            })
            .collect();
        let mut transfer = EnrichmentTransfer::default();
        for slot in &slots {
            transfer.add_slot(slot);
        }
        drop(captured_guards);

        let previous_guards: Vec<_> = slots
            .iter()
            .enumerate()
            .map(|(index, slot)| {
                slot.push(Arc::from(vec![EnrichmentEntry::unclassified(
                    "previous",
                    i64::try_from(index).unwrap(),
                )]))
            })
            .collect();

        {
            let _guard = transfer.apply();
            for slot in &slots {
                assert_eq!(chain_entries(&slot.current())[0].key().as_str(), "captured");
            }
        }

        for slot in &slots {
            assert_eq!(chain_entries(&slot.current())[0].key().as_str(), "previous");
        }
        drop(previous_guards);
        for slot in &slots {
            assert!(slot.current().is_none());
        }
    }

    #[test]
    fn empty_transfer_does_not_materialize_enrichment() {
        let mut transfer = EnrichmentTransfer::default();

        transfer.push(UnreachableEnrichment);
        transfer.push_for(SinkId::new("target"), UnreachableEnrichment);
    }

    #[test]
    fn push_entries_ignores_an_empty_transfer() {
        let mut transfer = EnrichmentTransfer::default();
        let entries = Arc::from(vec![EnrichmentEntry::unclassified("ignored", 1_i64)]);

        transfer.push_entries(&entries);

        assert!(transfer.slots.is_empty());
    }

    #[test]
    fn debug_impls() {
        let slot = Slot::new();
        assert!(format!("{slot:?}").contains("Slot"));

        let guard = slot.push(Arc::from(vec![EnrichmentEntry::unclassified("k", 1_i64)]));
        assert!(format!("{guard:?}").contains("Guard"));
        drop(guard);

        let mut transfer = EnrichmentTransfer::default();
        transfer.add_slot(&slot);
        assert!(format!("{transfer:?}").contains("EnrichmentTransfer"));

        let applied_guard = transfer.apply();
        assert!(format!("{applied_guard:?}").contains("AppliedGuard"));
    }
}
