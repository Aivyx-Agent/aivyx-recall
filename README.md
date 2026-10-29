# aivyx-recall

[![CI](https://github.com/Aivyx-Agent/aivyx-recall/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Aivyx-Agent/aivyx-recall/actions/workflows/ci.yml)
[![License: BUSL-1.1](https://img.shields.io/badge/license-BUSL--1.1-blue.svg)](LICENSE)

Storage-agnostic cross-session memory substrate for Aivyx agents.

A `Recall` trait (`put`/`get_recent`/`forget`, topic-scoped, sequence-ordered)
plus two implementations: `InMemoryRecall` (a deterministic fake, for
tests) and `FileRecall` (the default persistent backend — one JSON file
per topic).

Deliberately minimal — no embeddings, no ranking, no encryption, no
capability/audit model of its own. Consumers layer whatever scoping,
security, and retrieval semantics they need on top of the same trait.
Used today by `aivyx-coder` (topic-scoped, on-demand recall tools).

`aivyx` (the sibling Aivyx Personal Assistant) was considered as a second
consumer — wrapping this crate with its own encrypted, capability-scoped
storage — but that migration was investigated (2026-08-10) and declined:
`aivyx`'s own `aivyx-memory` crate has grown an 18-method `Memory` trait
(search ranking, eviction, embeddings/ANN — all things this crate
deliberately doesn't own) and a global-per-substrate sequence counter,
where this crate's `seq` is deliberately per-topic. Not reconcilable
without either growing this crate into something it isn't, or changing
tested behavior in a mature, security-relevant system, for the sake of
sharing three methods. `aivyx-memory` stays on its own storage,
unchanged. This crate remains available for a future consumer whose
shape actually fits it.

See `docs/superpowers/specs/2026-08-09-aivyx-recall-design.md` in the
`aivyx-coder` repo for the full design rationale.

## Returned text is untrusted

`get_recent` returns stored bodies verbatim. Anything an agent saved in an
earlier session, including text it took from a web page or a file while
under an injection, comes back in later sessions. The crate does not fence
or scan it. A consumer must present recalled text as quoted data (a fenced
block that names its source) and keep running its injection scan on it.

## Known limitations

- `FileRecall::put` rejects a body over `MAX_BODY_BYTES` (64 KiB) and keeps
  at most 1000 entries per topic. `InMemoryRecall` is a test fake and has
  neither limit.
- Topic files are named from a sanitised prefix plus a 64-bit FNV-1a hash.
  If two different topic names ever map to the same file, `put` and `forget`
  fail with a backend error instead of overwriting the other topic's entries;
  there is no automatic resolution, so choose topic names that do not
  collide.
