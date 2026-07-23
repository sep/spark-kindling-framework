# Governed Artifact Platform

**Date:** 2026-07-22
**Status:** Concept (pre-proposal)
**Scope:** A layer above kindling for governed reference artifacts — versioned, schema-validated, testable domain data with a publish lifecycle — and the domain workbench experience it enables

---

## What This Is

This is a thought concept, not yet a proposal. It sketches a product layer one
to two levels of abstraction above kindling: a **domain-specific operational
and analytical workbench** in which domain experts — not just data engineers —
contribute governed inputs to pipelines without writing pipeline code.

The concrete scenario that motivates it:

> A domain engineer opens their workbench, explores some data in a notebook,
> and realizes one of their standardized reference mappings is wrong. They open
> the mapping — which is data, not code, backed by a JSON file but edited
> through a purpose-built UI — fix the entry, test the fix against real data in
> the same session, and publish. The change is versioned, affected pipelines
> reprocess, and downstream outputs are stamped with the new mapping version.

The unit of contribution in this scenario is not a notebook, not an entity
declaration, and not a pull request against pipeline code. It is a **governed
reference artifact**: a piece of domain data with a schema, validation rules,
a draft/test/publish lifecycle, known consumers, and provenance that follows
it through the pipeline.

Kindling is the substrate two layers down. The domain expert never sees an
entity tag; they see "my mapping, version 13."

---

## The Layering

The test that keeps this honest is a strict three-layer split:

| Layer | Name | Knows about | Examples |
| --- | --- | --- | --- |
| 0 | Kindling | Entities, pipes, the DAG, config, execution, provenance stamping, reprocessing | `derived_entity`, `PipeGraphBuilder`, `entity_provider_registry` |
| 1 | Governed artifact platform | Artifact types, schemas, versions, drafts, publish policy, impact preview, consumers | "Artifacts have schemas and consumers" — nothing else |
| 2 | Domain workbench | The actual artifact types, their validation rules, their editors, the domain vocabulary | A unit map, a tag mapping, a calibration table, an asset hierarchy |

**The leak test:** the *second* artifact type a domain team defines should cost
a schema, some validation rules, and a form — no platform work. If adding an
artifact type requires touching layer 1, the platform has leaked domain
knowledge and the layering has failed.

Layer 1 is deliberately domain-agnostic. It should not know what a "unit" is,
any more than kindling knows what a "customer" is. The domain opinions —
vocabulary, editors, rules like "every source unit maps to exactly one
standard unit" — live entirely in layer 2, defined by the team that owns the
domain.

---

## Core Concept: The Governed Reference Artifact

An artifact is domain data treated like code for its *workflow* and like data
for its *consumption*:

- **Backed by a versioned file** (JSON or similar) in a git-backed location.
  Git is the artifact store, not a compromise: versioning, diff, review,
  audit, and rollback come for free. Publish is a commit/tag (or an
  auto-merged PR when policy passes).
- **Typed by an artifact type**: a JSON Schema plus domain validation rules
  registered at layer 2. Validation runs at edit time (live, in the editor),
  at publish time (enforced), and in CI (enforced again on the promotion
  path).
- **Consumed as a kindling entity**: the published artifact is ingested as a
  reference entity, stamped with its version. Pipelines depend on it like any
  other entity, which means the pipe graph already knows its consumers.
- **Lifecycle, not CRUD**: draft → validate → test (impact preview) → publish
  → propagate (reprocess) → provenance. Every stage has a defined owner and an
  enforcement point.

This is distinct from — and complements — declaration authoring. Declarations
(entities, pipes, tags) are code-adjacent and governed at *promotion time*
via git/CI. Artifacts are data and governed at *write/publish time* via the
platform. Two chokepoints, one policy vocabulary.

---

## The Five Capabilities

The motivating scenario decomposes into five capabilities with very different
costs. Roughly in dependency order:

### 1. Provenance stamping (kindling feature — small, high leverage)

Outputs carry the artifact versions they were built with — a column or table
property such as `unit_map_version=13`, written at pipe execution time. This
is what makes a bad artifact *recoverable*: query which outputs were built
with v12, reprocess exactly those. It also gives auditors a direct answer.

This is a write-path feature in kindling's entity write flow, and the whole
story leans on it. It composes naturally with the tags-first convention: a
pipe that consumes a versioned reference entity stamps the version it read.

### 2. Versioned artifact store with publish workflow (platform — cheap)

Git-backed JSON files, one directory per artifact type, publish as
commit/tag. Policy decides whether publish requires human review (PR) or
auto-merges when validation passes. The platform provides the SDK/CLI verbs
(`artifact new / edit / validate / diff / publish / history / rollback`);
git provides the storage semantics.

### 3. Artifact types with schema + validation (platform — cheap)

An artifact type is a registration: name, JSON Schema, validation functions,
and (optionally) an editor descriptor for UI layers. Rules are implemented
once, in the platform SDK (Python), and reused everywhere: live diagnostics
in the editor, enforcement at publish, re-enforcement in CI. A rule that
exists only in a UI layer is decoration, not governance.

### 4. Draft pinning and impact preview (platform + kindling — medium)

"Test it" means: given a *draft* artifact, run the affected slice of the
pipeline against dev-scoped data and diff the output against current. Two
pieces:

- **Draft pinning**: a session-scoped override that resolves an artifact
  entity to a draft version instead of the published one — e.g.
  `workbench.use_draft("unit_map", my_draft)` in a notebook, then re-run the
  cells. This is the moment the product sells itself: discovery and fix happen
  in one sitting, in one tool.
- **Impact preview**: the pipe graph already answers "which pipes consume this
  entity"; impact preview composes that with draft pinning to execute the
  affected subgraph in a dev scope and present a diff.

### 5. Publish-triggered reprocessing (kindling feature — the hard one)

A published artifact version invalidates downstream data. Kindling has the
DAG and derived-entity replace semantics, but "data-dependency-triggered
selective reprocess" is a real roadmap item: which entities are invalidated,
over what time range, at what cost, triggered how?

Cost control is the trust boundary. A one-line mapping fix that silently
triggers a full-history rebuild of gold tables is how the platform loses its
operators. Reprocessing should be scoped (provenance makes "exactly the
outputs built with v12" queryable), estimated before execution, and gated by
policy above some cost threshold.

The scenario degrades gracefully without this capability — pipelines can be
re-run manually in the interim — which is why it comes last.

---

## Surfaces

**SDK and CLI first.** Everything the platform does must be doable headless:
artifact CRUD, validation, draft pinning, impact preview, publish. This is
the contract CI enforces and agents drive. The CLI extends the existing
`kindling` CLI's conventions (JSON output, scaffolding verbs).

**The workbench (VS Code) is load-bearing UX, not chrome.** For the
motivating persona, the whole loop lives in one place: notebooks for
discovery, the artifact editor beside them, draft-pinned re-runs in the same
session, publish without leaving. That loop is the argument for a
domain-opinionated VS Code extension — it is the only surface where
discovery → fix → test → publish is one sitting. The extension renders
layer-2 editor descriptors as forms, surfaces layer-1 validation as live
diagnostics, and calls the same SDK/CLI for everything.

**A hosted API is an open question, not a prerequisite.** Git-backed
artifacts with CI enforcement may be sufficient governance for a long time.
A hosted service (a Databricks App is the natural host) becomes necessary
only if artifacts need write-time enforcement that a promotion path cannot
provide — e.g. approval workflows with domain-specific reviewers, or
non-git-literate contributors. Defer until a concrete need forces it.

---

## What Kindling Itself Needs

The platform asks three things of kindling, in ascending size:

1. **Provenance stamping** — stamp consumed reference-entity versions on
   outputs at write time. Small write-path feature; enables recovery and
   scoped reprocessing.
2. **Draft/override resolution for reference entities** — a session-scoped
   mechanism to resolve an entity to an alternate (draft) source. Likely
   composes with the existing entity provider registry and `entity_tags`
   config override path.
3. **Selective reprocessing** — given "entity X changed as of version N,"
   compute and execute the invalidated downstream slice with cost estimation.
   The largest design surface; overlaps with existing watermark and
   derived-entity replace semantics and deserves its own proposal.

None of these are workbench features. They are kindling features that happen
to be motivated by the workbench, and each is independently useful.

---

## What This Is Not

- **Not a metadata catalog.** Unity Catalog and friends describe data that
  exists; this governs domain-authored *inputs* with a lifecycle.
- **Not declaration tooling.** Authoring entities/pipes is code-adjacent work
  governed at promotion time; that is a separate (adjacent) tooling story.
- **Not a second framework.** Layer 1 owns lifecycle and policy plumbing
  only; execution, dependency, and storage semantics stay in kindling.
- **Not VS-Code-dependent.** The extension is the best surface for the core
  loop, but the SDK/CLI carries every capability headless.

---

## Open Questions

1. **Artifact identity and referencing** — how does a pipe declare a
   dependency on an artifact at a version constraint ("latest published" vs
   pinned)? Tags seem like the natural surface (`artifact.type`,
   `artifact.version`), per the tags-first convention.
2. **Environment promotion** — does an artifact version publish once and
   promote through environments (dev → test → prod) like code, or publish
   per-environment? The git-backed model suggests promote-like-code.
3. **Large artifacts** — JSON-in-git works for mappings measured in KB–MB.
   Reference data measured in GB needs a Delta-backed variant with the same
   lifecycle; the seed-rows work (`memory_provider_seed_rows.md`) is
   adjacent.
4. **Concurrent drafts** — two engineers drafting against the same artifact:
   git branching answers storage, but the UX for merge/conflict in a form
   editor is unexplored.
5. **Reprocessing policy** — who approves a reprocess above a cost
   threshold, and where does that approval live if there is no hosted
   service?

---

## Graduation Criteria

This concept becomes a proposal (or several) when:

- Provenance stamping is specced against kindling's current write path.
- Draft resolution is specced against the entity provider registry.
- One real artifact type from a real domain is defined end-to-end on paper
  (schema, rules, editor sketch, consumer pipes) and survives the leak test.

The likely decomposition is three proposals: provenance stamping (kindling),
draft/override entity resolution (kindling), and the artifact platform SDK/CLI
(new package), with selective reprocessing following as a fourth once the
first three exist.

---

## Related Documents

- `docs/contributing/declaration_conventions.md` — tags-first convention the
  artifact surface should follow
- `docs/proposals/kindling_core_runner_split.md` — the design-time/runtime
  split this platform would build on
- `docs/proposals/memory_provider_seed_rows.md` — adjacent: seeded reference
  data as entities
- `docs/proposals/kindling_patterns_cli.md` — precedent for opinionated
  layers above raw config
- `docs/proposals/config_driven_execution_options.md` — config-first
  execution principle the platform inherits
