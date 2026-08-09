# aivyx-recall

Storage-agnostic cross-session memory substrate for Aivyx agents.

A `Recall` trait (`put`/`get_recent`/`forget`, topic-scoped, sequence-ordered)
plus two implementations: `InMemoryRecall` (a deterministic fake, for
tests) and `FileRecall` (the default persistent backend — one JSON file
per topic).

Deliberately minimal — no embeddings, no ranking, no encryption, no
capability/audit model of its own. Consumers layer whatever scoping,
security, and retrieval semantics they need on top of the same trait.
Used by `aivyx-coder` (topic-scoped, on-demand recall tools) and,
eventually, `aivyx` (wrapping it with its own encrypted, capability-scoped
storage).

See `docs/superpowers/specs/2026-08-09-aivyx-recall-design.md` in the
`aivyx-coder` repo for the full design rationale.
