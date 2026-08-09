use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{Recall, RecallEntry, RecallError};

/// The default persistent `Recall` backend: one JSON file per topic under
/// `dir`. Filename = a sanitized topic prefix + an FNV-1a hash of the full
/// topic string — the same scheme `aivyx-coder`'s own session-persistence
/// layer uses for its session keys (inlined FNV-1a, not `std`'s
/// `DefaultHasher`, which doesn't guarantee stability across program
/// versions; the filename must stay stable across restarts and upgrades).
/// Written via `std::fs::write` then `chmod 0600` on Unix — direct write,
/// not atomic-via-tempfile-rename, matching that same layer's accepted
/// best-effort tradeoff (a torn write from a mid-write crash loses that
/// one topic's file, not more).
///
/// A single `tokio::sync::Mutex` serializes every read-modify-write across
/// every topic — simple and correct at personal-memory scale; no
/// per-topic lock bookkeeping.
pub struct FileRecall {
    dir: PathBuf,
    lock: Mutex<()>,
}

#[derive(Serialize, Deserialize, Default)]
struct TopicFile {
    entries: Vec<RecallEntry>,
}

impl FileRecall {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            lock: Mutex::new(()),
        }
    }

    fn topic_path(&self, topic: &str) -> PathBuf {
        let sanitized: String = topic
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .take(40)
            .collect();
        self.dir
            .join(format!("{sanitized}-{:016x}.json", fnv1a(topic.as_bytes())))
    }

    fn load(&self, topic: &str) -> Result<Vec<RecallEntry>, RecallError> {
        let path = self.topic_path(topic);
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                let mut file: TopicFile =
                    serde_json::from_str(&raw).map_err(|e| RecallError::Encoding(e.to_string()))?;
                // Defense against a filename collision (sanitized prefix + FNV-1a hash both
                // matching for two different topic strings): FNV-1a isn't collision-resistant,
                // so without this filter a colliding `get_recent` on topic B could silently
                // return topic A's entries too, and `forget`'s reported count would include
                // them. This filter closes the read-side leak and makes that count honest —
                // it does NOT protect the colliding entries from being destroyed: `forget`
                // still unlinks the whole shared file, and `put` still writes back only the
                // filtered set, discarding whatever it filtered out. True collisions are
                // birthday-negligible and structurally can't cross project/namespace
                // boundaries, so this residual risk is accepted rather than engineered away
                // (e.g. by moving to per-entry files or a real collision-resolution scheme).
                file.entries.retain(|e| e.topic == topic);
                Ok(file.entries)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(RecallError::Backend(err.to_string())),
        }
    }

    fn save(&self, topic: &str, entries: &[RecallEntry]) -> Result<(), RecallError> {
        std::fs::create_dir_all(&self.dir).map_err(|e| RecallError::Backend(e.to_string()))?;
        let path = self.topic_path(topic);
        let json = serde_json::to_string_pretty(&TopicFile {
            entries: entries.to_vec(),
        })
        .map_err(|e| RecallError::Encoding(e.to_string()))?;
        std::fs::write(&path, json).map_err(|e| RecallError::Backend(e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }
}

#[async_trait]
impl Recall for FileRecall {
    async fn put(&self, topic: &str, body: &str) -> Result<u64, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        let _guard = self.lock.lock().await;
        let mut entries = self.load(topic)?;
        let seq = entries
            .iter()
            .map(|e| e.seq)
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);
        let created_at_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        entries.push(RecallEntry {
            topic: topic.to_string(),
            body: body.to_string(),
            seq,
            created_at_secs,
        });
        self.save(topic, &entries)?;
        Ok(seq)
    }

    async fn get_recent(&self, topic: &str, limit: usize) -> Result<Vec<RecallEntry>, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        if limit == 0 {
            return Err(RecallError::ZeroLimit);
        }
        let _guard = self.lock.lock().await;
        let mut entries = self.load(topic)?;
        entries.sort_by_key(|e| std::cmp::Reverse(e.seq));
        entries.truncate(limit);
        Ok(entries)
    }

    async fn forget(&self, topic: &str) -> Result<usize, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        let _guard = self.lock.lock().await;
        let entries = self.load(topic)?;
        let count = entries.len();
        if count > 0 {
            std::fs::remove_file(self.topic_path(topic))
                .map_err(|e| RecallError::Backend(e.to_string()))?;
        }
        Ok(count)
    }
}

/// Must stay byte-identical to `aivyx-coder`'s `crates/aivyx-core/src/session.rs::fnv1a` —
/// see that function's own doc comment for why `std::collections::hash_map::DefaultHasher`
/// isn't used here.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::assert_conformance;

    #[tokio::test]
    async fn satisfies_the_recall_contract() {
        let dir = tempfile::tempdir().unwrap();
        let recall = FileRecall::new(dir.path());
        assert_conformance(&recall).await;
    }

    #[tokio::test]
    async fn entries_persist_across_a_fresh_instance_pointed_at_the_same_dir() {
        let dir = tempfile::tempdir().unwrap();
        {
            let recall = FileRecall::new(dir.path());
            recall.put("topic", "remember this").await.unwrap();
        }
        let reopened = FileRecall::new(dir.path());
        let entries = reopened.get_recent("topic", 10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].body, "remember this");
    }

    #[tokio::test]
    async fn load_filters_out_entries_from_a_different_topic_sharing_the_same_file() {
        // Simulates a filename collision (sanitized-prefix + FNV-1a hash both matching for
        // two distinct topic strings) by writing a `TopicFile` with mixed-topic entries
        // straight to the path `topic_path` computes for "topic-a", then confirming
        // `get_recent` for "topic-a" returns only its own entries, not "topic-b"'s.
        let dir = tempfile::tempdir().unwrap();
        let recall = FileRecall::new(dir.path());
        let path = recall.topic_path("topic-a");
        std::fs::create_dir_all(dir.path()).unwrap();
        let mixed = TopicFile {
            entries: vec![
                RecallEntry {
                    topic: "topic-a".to_string(),
                    body: "belongs to a".to_string(),
                    seq: 0,
                    created_at_secs: 0,
                },
                RecallEntry {
                    topic: "topic-b".to_string(),
                    body: "belongs to b, should never surface for a".to_string(),
                    seq: 1,
                    created_at_secs: 0,
                },
            ],
        };
        std::fs::write(&path, serde_json::to_string_pretty(&mixed).unwrap()).unwrap();

        let entries = recall.get_recent("topic-a", 10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].topic, "topic-a");
        assert_eq!(entries[0].body, "belongs to a");
    }

    #[test]
    #[cfg(unix)]
    fn topic_file_is_written_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let recall = FileRecall::new(dir.path());
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(recall.put("topic", "secret"))
            .unwrap();
        let path = recall.topic_path("topic");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
