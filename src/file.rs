use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
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
///
/// `dir` is created with owner-only (`0700`) permissions at creation time
/// (not `chmod`-ed after the fact), and every file this backend creates —
/// topic files, the lock file below, and atomic-write temp files — is
/// opened with owner-only (`0600`) permissions from the moment it's
/// created, for the same reason.
///
/// Topic files are written atomically: a `put`/`forget` writes the new
/// content to a temp file in the same directory, then `rename`s it over
/// the topic file. A concurrent `get_recent` reading the same path
/// therefore always sees either the complete old file or the complete new
/// one, never a torn write — unlike a direct `std::fs::write`, which can
/// leave a reader looking at a half-written file.
///
/// Two separate processes (e.g. two `aivyx-coder` terminals) can point
/// `FileRecall` at the same directory. To keep a `put`/`forget`'s
/// read-modify-write cycle from racing a concurrent process's own cycle on
/// the same topic — which would otherwise lose entries and could hand out
/// duplicate `seq` values — every read-modify-write additionally takes an
/// exclusive advisory lock (`flock`, via the small, pure-Rust [`fd-lock`]
/// crate rather than hand-rolling `libc::flock`: no `unsafe` in its public
/// API, and it already covers the Windows `LockFileEx` equivalent for
/// free) on a dedicated lock file in `dir`. That lock-acquire-and-hold
/// span runs on a blocking thread (`tokio::task::spawn_blocking`), since
/// `flock` blocks the OS thread rather than yielding to the async
/// executor, nested inside the existing in-process `tokio::sync::Mutex` so
/// only one task in *this* process does that blocking work at a time.
/// `get_recent` is read-only and doesn't need the cross-process lock —
/// atomic rename alone is what makes concurrent reads safe — but still
/// takes the in-process `Mutex`, as before.
///
/// [`fd-lock`]: https://docs.rs/fd-lock
///
/// A topic file that fails to parse as JSON (e.g. truncated by a crash
/// outside of this backend's own atomic-write path, or edited by hand)
/// doesn't permanently block that topic: `get_recent` logs a warning and
/// returns an empty list, `forget` logs a warning, deletes the file, and
/// reports `0` entries forgotten, and `put` logs a warning, moves the file
/// aside to `<name>.corrupt-<unix-secs>`, and starts the topic fresh (at
/// `seq = 0`).
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

    #[cfg(test)]
    fn topic_path(&self, topic: &str) -> PathBuf {
        topic_path_in(&self.dir, topic)
    }

    fn lock_path(&self) -> PathBuf {
        self.dir.join(".aivyx-recall.lock")
    }

    /// Runs `f` — synchronous, filesystem-bound work for one `put` or
    /// `forget` — on a blocking thread while holding both the in-process
    /// `Mutex` and an exclusive cross-process `flock` on this instance's
    /// lock file. See the struct doc comment for why both are needed.
    async fn with_exclusive_lock<F, T>(&self, f: F) -> Result<T, RecallError>
    where
        F: FnOnce() -> Result<T, RecallError> + Send + 'static,
        T: Send + 'static,
    {
        let _guard = self.lock.lock().await;
        let lock_path = self.lock_path();
        tokio::task::spawn_blocking(move || {
            ensure_dir(lock_path.parent().unwrap_or(Path::new(".")))?;
            let lock_file = open_owner_only(&lock_path)?;
            let mut rw = fd_lock::RwLock::new(lock_file);
            let _write_guard = rw
                .write()
                .map_err(|e| RecallError::Backend(format!("failed to acquire recall lock: {e}")))?;
            f()
        })
        .await
        .map_err(|e| RecallError::Backend(format!("recall lock task panicked: {e}")))?
    }
}

#[async_trait]
impl Recall for FileRecall {
    async fn put(&self, topic: &str, body: &str) -> Result<u64, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        let dir = self.dir.clone();
        let topic_owned = topic.to_string();
        let body_owned = body.to_string();
        self.with_exclusive_lock(move || {
            let mut entries = match load(&dir, &topic_owned) {
                Ok(entries) => entries,
                Err(RecallError::Encoding(reason)) => {
                    quarantine_unparseable(&dir, &topic_owned, &reason)?;
                    Vec::new()
                }
                Err(e) => return Err(e),
            };
            let seq = entries
                .iter()
                .map(|e| e.seq)
                .max()
                .map(|m| m + 1)
                .unwrap_or(0);
            let created_at_secs = now_secs();
            entries.push(RecallEntry {
                topic: topic_owned.clone(),
                body: body_owned,
                seq,
                created_at_secs,
            });
            save(&dir, &topic_owned, &entries)?;
            Ok(seq)
        })
        .await
    }

    async fn get_recent(&self, topic: &str, limit: usize) -> Result<Vec<RecallEntry>, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        if limit == 0 {
            return Err(RecallError::ZeroLimit);
        }
        let _guard = self.lock.lock().await;
        let mut entries = match load(&self.dir, topic) {
            Ok(entries) => entries,
            Err(RecallError::Encoding(reason)) => {
                eprintln!(
                    "warning: aivyx-recall: topic {topic:?} failed to parse ({reason}); \
                     returning an empty list instead of erroring"
                );
                Vec::new()
            }
            Err(e) => return Err(e),
        };
        entries.sort_by_key(|e| std::cmp::Reverse(e.seq));
        entries.truncate(limit);
        Ok(entries)
    }

    async fn forget(&self, topic: &str) -> Result<usize, RecallError> {
        if topic.is_empty() {
            return Err(RecallError::EmptyTopic);
        }
        let dir = self.dir.clone();
        let topic_owned = topic.to_string();
        self.with_exclusive_lock(move || match load(&dir, &topic_owned) {
            Ok(entries) => {
                let count = entries.len();
                if count > 0 {
                    remove_file_if_exists(&topic_path_in(&dir, &topic_owned))?;
                }
                Ok(count)
            }
            Err(RecallError::Encoding(reason)) => {
                eprintln!(
                    "warning: aivyx-recall: topic {topic_owned:?} failed to parse ({reason}); \
                     deleting it and reporting 0 entries forgotten"
                );
                remove_file_if_exists(&topic_path_in(&dir, &topic_owned))?;
                Ok(0)
            }
            Err(e) => Err(e),
        })
        .await
    }
}

fn topic_path_in(dir: &Path, topic: &str) -> PathBuf {
    let sanitized: String = topic
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(40)
        .collect();
    dir.join(format!("{sanitized}-{:016x}.json", fnv1a(topic.as_bytes())))
}

fn load(dir: &Path, topic: &str) -> Result<Vec<RecallEntry>, RecallError> {
    let path = topic_path_in(dir, topic);
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

fn save(dir: &Path, topic: &str, entries: &[RecallEntry]) -> Result<(), RecallError> {
    ensure_dir(dir)?;
    let path = topic_path_in(dir, topic);
    let json = serde_json::to_string_pretty(&TopicFile {
        entries: entries.to_vec(),
    })
    .map_err(|e| RecallError::Encoding(e.to_string()))?;
    write_atomically(dir, &path, json.as_bytes())
}

/// Writes `contents` to `final_path` without a reader ever being able to
/// observe a partial write: writes to an owner-only temp file in the same
/// directory (so the final `rename` is a same-filesystem, POSIX-atomic
/// replace rather than a cross-device copy), then renames it over
/// `final_path`.
fn write_atomically(dir: &Path, final_path: &Path, contents: &[u8]) -> Result<(), RecallError> {
    let file_name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_path = dir.join(format!(".{file_name}.tmp-{}-{nanos}", std::process::id()));
    {
        let mut file = open_owner_only(&tmp_path)?;
        file.write_all(contents)
            .map_err(|e| RecallError::Backend(e.to_string()))?;
        file.sync_all()
            .map_err(|e| RecallError::Backend(e.to_string()))?;
    }
    std::fs::rename(&tmp_path, final_path).map_err(|e| RecallError::Backend(e.to_string()))
}

/// Moves a topic file that failed to parse aside to
/// `<name>.corrupt-<unix-secs>` so `put` can start the topic fresh instead
/// of being blocked on it forever. A no-op (not an error) if the file is
/// already gone.
fn quarantine_unparseable(dir: &Path, topic: &str, reason: &str) -> Result<(), RecallError> {
    let path = topic_path_in(dir, topic);
    let Some(file_name) = path.file_name().map(|n| n.to_string_lossy().into_owned()) else {
        return Ok(());
    };
    let quarantined = dir.join(format!("{file_name}.corrupt-{}", now_secs()));
    match std::fs::rename(&path, &quarantined) {
        Ok(()) => {
            eprintln!(
                "warning: aivyx-recall: topic {topic:?} failed to parse ({reason}); moved \
                 aside to {} and starting the topic fresh",
                quarantined.display()
            );
            Ok(())
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(RecallError::Backend(e.to_string())),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<(), RecallError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(RecallError::Backend(e.to_string())),
    }
}

/// Creates `dir` (and any missing parents) with owner-only (`0700`)
/// permissions set at creation time. A no-op, not an error, if `dir`
/// already exists.
fn ensure_dir(dir: &Path) -> Result<(), RecallError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder.recursive(true);
        builder
            .create(dir)
            .map_err(|e| RecallError::Backend(e.to_string()))
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(dir).map_err(|e| RecallError::Backend(e.to_string()))
    }
}

/// Opens `path` for writing, creating it with owner-only (`0600`)
/// permissions if it doesn't already exist. The mode is only applied at
/// creation (never via a later `chmod`), and is a no-op if `path` already
/// exists with different permissions.
fn open_owner_only(path: &Path) -> Result<std::fs::File, RecallError> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|e| RecallError::Backend(e.to_string()))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

    #[test]
    #[cfg(unix)]
    fn memory_dir_is_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let parent = tempfile::tempdir().unwrap();
        let dir = parent.path().join("memories");
        let recall = FileRecall::new(&dir);
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(recall.put("topic", "secret"))
            .unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    // --- Task 1: lost writes across processes --------------------------------

    #[test]
    fn concurrent_puts_from_two_instances_lose_no_entries_and_keep_unique_seqs() {
        // Two separate `FileRecall` instances, each on its own OS thread with
        // its own single-threaded Tokio runtime, simulate two separate
        // `aivyx-coder` processes sharing one memory directory. Real OS-thread
        // parallelism (not just two tasks on one runtime, which never
        // actually interleave mid-operation since nothing here yields) is
        // what gives this test a genuine race window: without a
        // cross-process lock, interleaved read-modify-write cycles lose
        // entries and can hand out duplicate `seq` values.
        let dir = tempfile::tempdir().unwrap();
        const PER_INSTANCE: usize = 150;

        let spawn_instance = |label: &'static str| {
            let dir_path = dir.path().to_path_buf();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let recall = FileRecall::new(&dir_path);
                rt.block_on(async {
                    let mut seqs = Vec::with_capacity(PER_INSTANCE);
                    for i in 0..PER_INSTANCE {
                        seqs.push(
                            recall
                                .put("shared-topic", &format!("from-{label}-{i}"))
                                .await
                                .unwrap(),
                        );
                    }
                    seqs
                })
            })
        };

        let a = spawn_instance("a");
        let b = spawn_instance("b");
        let mut seqs = a.join().unwrap();
        seqs.extend(b.join().unwrap());

        let recall = FileRecall::new(dir.path());
        let entries = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(recall.get_recent("shared-topic", 10_000))
            .unwrap();

        // Every put's entry is present...
        assert_eq!(
            entries.len(),
            PER_INSTANCE * 2,
            "lost writes: expected {} entries, found {}",
            PER_INSTANCE * 2,
            entries.len()
        );

        // ...and every seq value handed out is unique.
        let unique: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        assert_eq!(
            unique.len(),
            seqs.len(),
            "duplicate seq values handed out: {seqs:?}"
        );
    }

    // --- Task 2: torn reads ---------------------------------------------------

    #[test]
    fn concurrent_readers_never_see_a_torn_write() {
        // Writer and readers each run on their own OS thread (own runtime,
        // own `FileRecall` instance pointed at the same dir) so the reads
        // genuinely race the writes at the syscall level, rather than being
        // cooperatively scheduled on one runtime.
        let dir = tempfile::tempdir().unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(FileRecall::new(dir.path()).put("topic", "seed"))
            .unwrap();

        const ROUNDS: usize = 300;
        let dir_path = dir.path().to_path_buf();

        let writer_dir = dir_path.clone();
        let writer = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let recall = FileRecall::new(&writer_dir);
            rt.block_on(async {
                for i in 0..ROUNDS {
                    // A longer body means more entries accumulate and the
                    // JSON file grows larger each round, widening the
                    // syscall window in which a non-atomic write could be
                    // observed half-done.
                    let body = "x".repeat(200);
                    recall.put("topic", &format!("{i}-{body}")).await.unwrap();
                }
            });
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_dir = dir_path.clone();
            readers.push(std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let recall = FileRecall::new(&reader_dir);
                rt.block_on(async {
                    for _ in 0..ROUNDS {
                        // A torn read (partial file content) would fail to
                        // parse as JSON and surface as `RecallError::Encoding`.
                        recall.get_recent("topic", 10).await.unwrap();
                    }
                });
            }));
        }

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
    }

    // --- Task 3: a corrupt topic file doesn't permanently block the topic ----

    fn write_garbage(recall: &FileRecall, topic: &str) {
        let path = recall.topic_path(topic);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not valid json {{{").unwrap();
    }

    #[tokio::test]
    async fn forget_deletes_an_unparseable_topic_file_and_reports_zero() {
        let dir = tempfile::tempdir().unwrap();
        let recall = FileRecall::new(dir.path());
        write_garbage(&recall, "broken");

        let deleted = recall.forget("broken").await.unwrap();
        assert_eq!(deleted, 0);
        assert!(!recall.topic_path("broken").exists());
    }

    #[tokio::test]
    async fn get_recent_on_an_unparseable_topic_file_returns_empty_list() {
        let dir = tempfile::tempdir().unwrap();
        let recall = FileRecall::new(dir.path());
        write_garbage(&recall, "broken");

        let entries = recall.get_recent("broken", 10).await.unwrap();
        assert_eq!(entries, vec![]);
    }

    #[tokio::test]
    async fn put_quarantines_an_unparseable_topic_file_and_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let recall = FileRecall::new(dir.path());
        write_garbage(&recall, "broken");

        let seq = recall.put("broken", "fresh start").await.unwrap();
        assert_eq!(seq, 0, "topic should restart at seq 0 after quarantine");

        let entries = recall.get_recent("broken", 10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].body, "fresh start");

        // The unparseable file was moved aside, not silently overwritten.
        let quarantined: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
            .collect();
        assert_eq!(
            quarantined.len(),
            1,
            "expected exactly one quarantined file"
        );
    }
}
