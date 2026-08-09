use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::{Recall, RecallEntry, RecallError};

/// Deterministic in-process fake, no persistence. For tests in this crate
/// and in consumers that don't want real filesystem I/O.
#[derive(Default)]
pub struct InMemoryRecall {
    entries: Mutex<HashMap<String, Vec<RecallEntry>>>,
}

impl InMemoryRecall {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Recall for InMemoryRecall {
    async fn put(&self, topic: &str, body: &str) -> Result<u64, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        let mut map = self.entries.lock().unwrap();
        let list = map.entry(topic.to_string()).or_default();
        let seq = list.iter().map(|e| e.seq).max().map(|m| m + 1).unwrap_or(0);
        let created_at_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        list.push(RecallEntry {
            topic: topic.to_string(),
            body: body.to_string(),
            seq,
            created_at_secs,
        });
        Ok(seq)
    }

    async fn get_recent(&self, topic: &str, limit: usize) -> Result<Vec<RecallEntry>, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        if limit == 0 {
            return Err(RecallError::ZeroLimit);
        }
        let map = self.entries.lock().unwrap();
        let mut matching: Vec<RecallEntry> = map.get(topic).cloned().unwrap_or_default();
        matching.sort_by(|a, b| b.seq.cmp(&a.seq));
        matching.truncate(limit);
        Ok(matching)
    }

    async fn forget(&self, topic: &str) -> Result<usize, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        Ok(self
            .entries
            .lock()
            .unwrap()
            .remove(topic)
            .map(|v| v.len())
            .unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::assert_conformance;

    #[tokio::test]
    async fn satisfies_the_recall_contract() {
        assert_conformance(&InMemoryRecall::new()).await;
    }
}
