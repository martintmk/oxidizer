// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Enrichment derive trait for typed enrichment structs.

use performables::arc::Arc;

use crate::enrichment::EnrichmentEntry;

/// Trait implemented by typed enrichment structs that convert into enrichment entries.
///
/// Derive this trait via `#[derive(Enrichment)]`. Unlike [`Event`](crate::Event), enrichment
/// structs have no severity, body, or metrics - they only produce key-value
/// enrichment entries that are attached to all events in scope.
///
/// See also: [`EnrichFutureExt::enrich`](crate::enrichment::EnrichFutureExt::enrich)
/// and [`EnrichFutureExt::enrich_for`](crate::enrichment::EnrichFutureExt::enrich_for).
///
/// See the [`Enrichment` derive macro](crate::Enrichment) for field attributes
/// and usage examples.
pub trait Enrichment {
    /// Converts this enrichment struct into an iterator of owned [`EnrichmentEntry`] items.
    ///
    /// The returned order is preserved when the entries are attached to a scope.
    /// Implementations can return an array's iterator instead of allocating a vector.
    fn into_entries(self) -> impl Iterator<Item = EnrichmentEntry>;
}

pub(super) fn collect_entries(entries: impl Iterator<Item = EnrichmentEntry>) -> Option<Arc<[EnrichmentEntry]>> {
    if entries.size_hint().0 > 0 {
        return Some(entries.collect());
    }
    let mut entries = entries.peekable();
    entries.peek()?;
    Some(entries.collect())
}
