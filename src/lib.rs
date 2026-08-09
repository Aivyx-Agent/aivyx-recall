//! Storage-agnostic cross-session memory substrate. See
//! `Recall`'s own doc comment for the contract every implementation
//! (this crate's own, or a consumer's) must satisfy.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod in_memory;
pub use in_memory::InMemoryRecall;

#[cfg(test)]
mod conformance;

/// A single stored memory entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallEntry {
    /// Non-empty UTF-8 topic chosen by the caller. Entries with the same
    /// topic come back together, newest first, from `get_recent`.
    pub topic: String,
    /// UTF-8 body — whatever the caller asked to remember.
    pub body: String,
    /// Monotonic **per-topic** insertion counter, assigned by the
    /// substrate. First entry under a topic gets `seq = 0`, the next
    /// `seq = 1`, and so on — restarting at 0 if the topic is later
    /// `forget`-ten and written again. (This is a deliberate divergence
    /// from a single substrate-wide counter: it keeps a one-file-per-topic
    /// backend like `FileRecall` simple, since each topic's next `seq` is
    /// derivable from that topic's own file alone, with no shared
    /// cross-topic counter to persist or race.)
    pub seq: u64,
    /// Wall-clock seconds since UNIX epoch, captured by the substrate at
    /// write time.
    pub created_at_secs: u64,
}

/// Errors the memory substrate can return.
#[derive(Debug, Error)]
pub enum RecallError {
    #[error("recall topic must be non-empty")]
    EmptyTopic,
    #[error("recall get_recent limit must be > 0")]
    ZeroLimit,
    #[error("recall entry encoding error: {0}")]
    Encoding(String),
    #[error("recall backend error: {0}")]
    Backend(String),
}

/// The minimum memory substrate surface. Implementations are expected to
/// be called from multiple tasks concurrently (`Send + Sync`).
///
/// Contract every implementation must satisfy (exercised by
/// `conformance::assert_conformance`, run against every implementation in
/// this crate):
/// - `put`/`get_recent`/`forget` all fail fast with `EmptyTopic` on an
///   empty topic string.
/// - `get_recent` additionally fails with `ZeroLimit` when `limit == 0`.
/// - An unwritten topic's `get_recent` returns an empty `Vec`, not an
///   error.
/// - `get_recent` returns up to `limit` entries, newest first (`seq`
///   descending).
/// - `forget` deletes every entry under a topic and returns how many were
///   deleted; forgetting an already-empty topic returns `0`, not an
///   error.
/// - Topics are independent: writing to one never affects another.
#[async_trait]
pub trait Recall: Send + Sync {
    async fn put(&self, topic: &str, body: &str) -> Result<u64, RecallError>;
    async fn get_recent(&self, topic: &str, limit: usize) -> Result<Vec<RecallEntry>, RecallError>;
    async fn forget(&self, topic: &str) -> Result<usize, RecallError>;
}
