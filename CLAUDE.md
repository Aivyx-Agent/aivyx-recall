# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`aivyx-recall` is a small, storage-agnostic cross-session memory
substrate: a `Recall` trait (`put`/`get_recent`/`forget`, topic-scoped,
sequence-ordered) plus two implementations. Deliberately minimal — no
embeddings, no ranking, no encryption, no capability/audit model of its
own. It exists specifically to be shared: `aivyx-coder` (a sibling repo in
this same workspace) depends on it via a pinned `git` dependency for its
`memory_read`/`memory_write`/`memory_forget` tools. `aivyx` (the larger
Aivyx Personal Assistant, also a sibling repo) has its own, much more
elaborate `aivyx-memory` crate (capability-scoped, encrypted, BM25/ANN
search) — **that crate has not been migrated onto this substrate yet**,
so don't assume `aivyx` actually consumes this crate today; see
"Consumers" below.

This crate has no CI workflow configured.

## Build, test, lint

```sh
cargo build
cargo test
cargo clippy --all-targets
cargo fmt
```

Single crate, no workspace — no `-p` flag needed. Single test:
`cargo test <test_name>`.

## Architecture

Everything lives in `src/`, four files:

- `lib.rs` — the trait (`Recall`), the entry type (`RecallEntry`), the
  error type (`RecallError`), and the *contract* every implementation
  must satisfy, documented once on `Recall`'s own doc comment rather than
  scattered across tests.
- `in_memory.rs` — `InMemoryRecall`, a `Mutex<HashMap<String,
  Vec<RecallEntry>>>` fake with no persistence. For this crate's own
  tests and for consumers that don't want real filesystem I/O in theirs.
- `file.rs` — `FileRecall`, the default persistent backend. One JSON file
  per topic; filename = a sanitized topic-string prefix + an FNV-1a hash
  of the full topic string. That hash function must stay byte-identical
  to `aivyx-coder`'s own `crates/aivyx-core/src/session.rs::fnv1a` — the
  two are a deliberate duplicate, not a shared dependency, because
  `aivyx-coder`'s `aivyx-tools` crate can't depend on `aivyx-core` (see
  that function's doc comment on both sides before changing either). A
  single `tokio::sync::Mutex` serializes every read-modify-write across
  all topics. Written via direct `std::fs::write` + `chmod 0600` on Unix
  — not atomic-via-tempfile-rename, an accepted tradeoff matching the
  same one `aivyx-coder`'s own session-persistence layer makes (a torn
  write from a mid-crash loses one topic's file, not more).
- `conformance.rs` — one async function, `assert_conformance`, run
  against every implementation as a `&dyn Recall` trait object, so
  `InMemoryRecall` and `FileRecall` both prove the identical behavioral
  contract rather than each carrying its own hand-rolled test suite. Any
  new `Recall` implementor should be tested the same way.

### The `Recall` contract

Three methods, deliberately minimal — no `list_topics`, no cross-topic
queries, no embeddings/ranking:

- `put(topic, body) -> seq` — fails fast on an empty topic.
- `get_recent(topic, limit) -> Vec<RecallEntry>` — newest first (`seq`
  descending); an unwritten topic returns an empty `Vec`, not an error;
  fails on empty topic or `limit == 0`.
- `forget(topic) -> count` — deletes every entry under a topic;
  forgetting an already-empty topic returns `0`, not an error.

`seq` is monotonic **per topic**, not per-substrate — a deliberate
divergence from `aivyx`'s own `aivyx-memory` crate (which uses a single
substrate-wide counter). Chosen because it keeps a one-file-per-topic
backend like `FileRecall` simple: each topic's next `seq` is derivable
from that topic's own file alone, with no shared cross-topic counter to
persist or race.

### `FileRecall`'s known, accepted residual risk

`load` filters loaded entries down to the requested topic before
returning them — defense against a filename collision, since the
sanitized-prefix + FNV-1a hash isn't collision-resistant. This closes the
*read*-side leak (a colliding `get_recent` silently returning another
topic's entries too) and keeps `forget`'s reported count honest. It does
**not** protect a colliding topic's entries from being *destroyed*:
`forget` still unlinks the whole shared file, and `put` still writes back
only the filtered set, discarding whatever it filtered out. This is
accepted as birthday-negligible rather than engineered away (e.g. via
per-entry files or a real collision-resolution scheme) — read the comment
directly above `entries.retain(...)` in `file.rs` before touching this
logic.

### Consumers, and what's deliberately *not* built here

Zero dependency on anything from either consumer's own architecture — no
`aivyx-storage`/`aivyx-crypto`/`aivyx-capability` (from `aivyx`), nothing
from `aivyx-coder`'s `aivyx-sandbox`/`aivyx-tools`. That's the whole
point of the extraction: scope, security, and retrieval semantics are
each consumer's own layer on top of this trait, not this crate's concern.

- **`aivyx-coder`** wraps `FileRecall` directly, adding its own
  topic-namespacing (`global:`/`project:` prefixes, resolved before ever
  reaching this crate) and its own permission-gate `ActionKind`. See
  `docs/superpowers/specs/2026-08-09-aivyx-recall-design.md` in the
  `aivyx-coder` repo for the full design rationale this crate was
  extracted for.
- **`aivyx`** does not use this crate, and — as of 2026-08-10 — is not
  expected to. Wrapping its `RedbMemory` around this crate's `Recall`
  trait was investigated as a follow-up and declined: `aivyx-memory`'s
  own `Memory` trait has grown to 18 methods (search ranking, eviction,
  `gc_*`, embeddings/ANN — all things this crate deliberately doesn't
  own), and its sequence counter is global-per-substrate where this
  crate's is deliberately per-topic (see `RecallEntry::seq`'s doc comment
  in `src/lib.rs`) — not reconcilable without either growing this crate
  into something its own design explicitly declines to be, or changing
  tested behavior in `aivyx-memory`. `aivyx-memory` stays on its own
  storage, unchanged. Don't propose this migration again without first
  re-reading `aivyx-memory`'s actual current `Memory` trait — if it's
  shrunk back toward the substrate-agnostic shape its own module docs
  describe, the calculus here may have changed.
