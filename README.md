# aivyx-recall

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
