---
plan_slug: autoloader-file-ingestion
phase: tasks
rig: kindling
rig_root: /workspaces/kindling
artifact_root: /workspaces/kindling/plans
requirements_file: /workspaces/kindling/plans/autoloader-file-ingestion/requirements.md
implementation_plan_file: /workspaces/kindling/plans/autoloader-file-ingestion/implementation-plan.md
status: created
created_at: '2026-08-06T16:55:02Z'
updated_at: '2026-08-06T18:38:58Z'
created_beads_at: '2026-08-06T18:38:58Z'
---

# Tasks: Wire Databricks Auto Loader into file ingestion

GitHub issue: https://github.com/sep/spark-kindling-framework/issues/228

## Task Plan

1. **Resolve open design decisions** — module placement, glob-vs-regex
   handling, checkpoint/schema-location convention, per-file signal
   semantics, schema-evolution field naming. Blocks everything else;
   `kindling_ext_databricks` currently has zero EntityProvider precedent,
   so this needs a real decision, not an assumption.
2. **Extract shared enrichment helper** from `_build_df_plan` (regex named
   groups + `static_values` + `ingestion_timestamp`) so both the existing
   batch path and the new Auto Loader `foreachBatch` path call the same
   code.
3. **Implement per-entry Auto Loader discovery path** — `discovery`
   opt-in field on `FileIngestionEntries.entry(...)`, per-entry
   `cloudFiles` stream with `Trigger.AvailableNow`, checkpoint/schema
   locations per the chosen convention, wired into
   `ParallelizingFileIngestionProcessor.process_path`.
4. **Wire schema-evolution opt-in policy** for Auto Loader entries.
5. **Tests**: unit coverage for the new `discovery` field and the shared
   enrichment helper; integration coverage for incremental discovery
   (no re-read across runs), unmatched-file skip behavior, and schema
   evolution.
6. **Docs**: document the `discovery="autoloader"` opt-in and the
   checkpoint/schema-location convention.

## Bead Creation Payload

```yaml
target_rig: kindling
labels:
  - autoloader
  - file-ingestion
convoys:
  - key: convoy-autoloader-ingestion
    title: "Wire Databricks Auto Loader into file ingestion"
    description: |
      Add a per-entry Auto Loader (cloudFiles) discovery path to
      FileIngestionEntry/ParallelizingFileIngestionProcessor, replacing
      full-directory-listing discovery for opted-in entries while leaving
      the existing batch path and write path unchanged. See GitHub issue
      #228 and plans/autoloader-file-ingestion/{requirements,implementation-plan}.md.
    metadata:
      github_issue: "228"
      plan_slug: autoloader-file-ingestion
    beads:
      - key: design-decisions
        title: "Resolve open design decisions for Auto Loader file ingestion"
        type: task
        priority: 1
        description: |
          Resolve the open questions in
          plans/autoloader-file-ingestion/implementation-plan.md before
          any code lands: (1) module/package home given
          kindling_ext_databricks currently has no EntityProvider/file-
          ingestion code at all -- only the Lakeflow SDP engine; (2)
          regex `patterns` (filename routing) vs. cloudFiles glob syntax
          -- same field, restricted subset, or a second field; (3)
          checkpoint/schema-location root convention (new ingestion.*
          config key vs. entity-tag derived); (4) per-file signal
          (before_file/after_file) semantics under foreachBatch; (5)
          schema-evolution policy field naming (reuse infer_schema vs.
          new field). Produce a short decision addendum appended to
          implementation-plan.md; do not silently pick answers in code
          without recording the reasoning.
        acceptance_criteria:
          - "Each of the 5 open questions in implementation-plan.md has a recorded decision with rationale"
          - "Decision addendum is appended to plans/autoloader-file-ingestion/implementation-plan.md"
        files:
          - plans/autoloader-file-ingestion/implementation-plan.md
        labels:
          - design
      - key: enrichment-helper
        title: "Extract shared file-ingestion enrichment helper"
        type: task
        priority: 2
        dependencies:
          - design-decisions
        description: |
          Extract the regex-named-group + static_values +
          ingestion_timestamp column-enrichment logic currently inlined
          in ParallelizingFileIngestionProcessor._build_df_plan
          (packages/kindling/file_ingestion.py, roughly lines 221-231)
          into a standalone helper function usable both from the
          existing batch path and from a future foreachBatch callback,
          without changing current batch-path behavior.
        acceptance_criteria:
          - "Existing batch-path behavior and existing tests are unaffected (no behavior change, pure extraction)"
          - "Helper is unit-testable in isolation from Spark's read path"
        files:
          - packages/kindling/file_ingestion.py
          - tests/unit/test_file_ingestion.py
        labels:
          - refactor
      - key: autoloader-provider
        title: "Implement per-entry Auto Loader discovery path"
        type: feature
        priority: 2
        dependencies:
          - enrichment-helper
        description: |
          Per the design decisions bead's outcome: add a `discovery`
          opt-in field to FileIngestionEntries.entry(...) (default
          "batch", preserving existing entries unchanged), and implement
          an Auto Loader-backed discovery path for
          discovery="autoloader" entries -- one cloudFiles stream per
          entry (own glob, own checkpointLocation, own
          cloudFiles.schemaLocation), run with Trigger.AvailableNow from
          process_path(), using foreachBatch to call the shared
          enrichment helper and the existing _write_table_group /
          EntityProvider.append_to_entity write path unchanged. Must fail
          fast (not silently fall back to batch) if the Databricks
          extension/cloudFiles isn't available. See
          plans/autoloader-file-ingestion/implementation-plan.md
          "Proposed Implementation" for the itemized design.
        acceptance_criteria:
          - "discovery=\"batch\" entries behave identically to current behavior (regression-safe default)"
          - "discovery=\"autoloader\" entries use cloudFiles with per-entry checkpoint and schema locations, not one shared stream over the landing path"
          - "A file matching no entry's glob/pattern is not read/parsed, matching today's listing-only-skip cost profile"
          - "Non-Databricks engines are unaffected; missing cloudFiles support fails fast with a clear error"
        files:
          - packages/kindling/file_ingestion.py
          - packages/extensions/kindling_ext_databricks/kindling_ext_databricks/
        labels:
          - databricks
      - key: schema-evolution
        title: "Wire schema-evolution opt-in policy for Auto Loader entries"
        type: feature
        priority: 3
        dependencies:
          - autoloader-provider
        description: |
          For discovery="autoloader" entries, wire an explicit
          schema-evolution policy (field name/values decided in the
          design-decisions bead) through to cloudFiles.schemaEvolutionMode
          instead of the current hardcoded inferSchema=false. Must be
          opt-in and explicit, not a silent behavior change for existing
          entries.
        acceptance_criteria:
          - "Schema-evolution behavior (evolve vs. fail, per chosen policy) is covered by an integration test with a new column appearing in a later file"
        files:
          - packages/kindling/file_ingestion.py
        labels:
          - databricks
      - key: tests
        title: "Tests for Auto Loader file-ingestion path"
        type: task
        priority: 2
        dependencies:
          - autoloader-provider
          - schema-evolution
        description: |
          Add unit coverage for the new discovery field/validation and
          the extracted enrichment helper, plus integration coverage
          (likely under tests/integration/ or a new
          kindling_ext_databricks test module) for: incremental discovery
          across repeated process_path() calls (no re-read of already-
          checkpointed files), unmatched-file skip behavior, and schema
          evolution. Confirm existing test_file_ingestion.py and
          test_entity_provider_parquet* suites remain green
          (discovery="batch" default path unaffected).
        acceptance_criteria:
          - "New unit tests for discovery field validation and enrichment helper pass"
          - "New integration test demonstrates no re-read of checkpointed files on a second process_path() call"
          - "Existing test_file_ingestion.py and related suites pass unmodified"
        verification:
          - "poe test-unit"
          - "poe test"
        files:
          - tests/unit/test_file_ingestion.py
          - tests/system/core/test_file_ingestion_static_values.py
        labels:
          - testing
      - key: docs
        title: "Document Auto Loader opt-in for file ingestion entries"
        type: task
        priority: 3
        dependencies:
          - autoloader-provider
        description: |
          Document the discovery="autoloader" opt-in, its config surface,
          and the checkpoint/schema-location convention chosen, in
          docs/contributing/entity_providers.md and/or a new file-
          ingestion guide.
        acceptance_criteria:
          - "docs describe when to use discovery=\"autoloader\" vs. the default batch path, and the checkpoint/schema-location convention"
        files:
          - docs/contributing/entity_providers.md
        labels:
          - docs
```

## Created Beads

| Key | Kind | Bead ID | Title |
|---|---|---|---|
| convoy-autoloader-ingestion | convoy | kind-88b | Wire Databricks Auto Loader into file ingestion |
| design-decisions | bead | kind-5i2 | Resolve open design decisions for Auto Loader file ingestion |
| enrichment-helper | bead | kind-3ez | Extract shared file-ingestion enrichment helper |
| autoloader-provider | bead | kind-45y | Implement per-entry Auto Loader discovery path |
| schema-evolution | bead | kind-d3w | Wire schema-evolution opt-in policy for Auto Loader entries |
| tests | bead | kind-iq3 | Tests for Auto Loader file-ingestion path |
| docs | bead | kind-27m | Document Auto Loader opt-in for file ingestion entries |
