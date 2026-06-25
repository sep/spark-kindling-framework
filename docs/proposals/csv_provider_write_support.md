# Proposal: CSV Provider Write Support

## Goal

Extend `CSVEntityProvider` to implement `WritableEntityProvider` (and optionally
`StreamWritableEntityProvider`) so that pipelines can write outputs to CSV files, not just read
from them.

## Motivation

The CSV provider is currently read-only by design, documented as suited only for
"reference/lookup data." But CSV is also a common exchange format for pipeline outputs: exporting
results for downstream consumers, producing test fixtures, and ETL workflows that hand off to
external systems. Without write support, teams must reach for a heavier provider (Delta, Memory)
or write ad-hoc Spark calls outside the entity framework.

## Current State

`CSVEntityProvider` (`packages/kindling/entity_provider_csv.py`) implements only
`BaseEntityProvider`:

| Interface | Implemented? |
|---|---|
| `BaseEntityProvider` (`read_entity`, `check_entity_exists`) | Yes |
| `WritableEntityProvider` (`write_to_entity`, `append_to_entity`) | No |
| `StreamWritableEntityProvider` (`append_as_stream`) | No |
| `DestinationEnsuringProvider` (`ensure_destination`) | No |

## Proposed Changes

### Batch write (required)

Add `WritableEntityProvider` to `CSVEntityProvider`. Both methods use Spark's CSV writer with the
same `provider.*` tag options already supported for reads, plus write-specific options:

| Tag | Default | Notes |
|---|---|---|
| `provider.path` | (required) | Output path — same key as read |
| `provider.header` | `"true"` | Write header row |
| `provider.delimiter` | `","` | Column delimiter |
| `provider.encoding` | `"UTF-8"` | Output encoding |
| `provider.quote` | `'"'` | Quote character |
| `provider.escape` | `"\\"` | Escape character |
| `provider.compression` | `"none"` | One of: none, gzip, bzip2, lz4, snappy, deflate |

`write_to_entity` — overwrite mode (`df.write.mode("overwrite").csv(path, ...)`).

`append_to_entity` — append mode (`df.write.mode("append").csv(path, ...)`).

### Streaming write (optional / separate follow-up)

Add `StreamWritableEntityProvider` using Spark's `writeStream` CSV sink. This is lower priority
since CSV streaming sinks produce many small part-files; flag as a follow-up unless there is a
clear use case.

### `ensure_destination`

Not needed — Spark's CSV writer creates the output directory automatically.

### Docstring update

Remove the "Does not support write operations" note from the class docstring and replace with a
summary of supported modes.

## Reference Implementation

`MemoryEntityProvider` (`packages/kindling/entity_provider_memory.py`, lines 273–394) is the
closest full-featured reference for implementing all write interfaces.

## Implementation Checklist

### 1) Implement `WritableEntityProvider`

- [ ] Add `WritableEntityProvider` to the class signature in `entity_provider_csv.py`.
- [ ] Implement `write_to_entity(df, entity_metadata)` — resolve path and write options from
  provider config, call `df.write.mode("overwrite").options(...).csv(path)`.
- [ ] Implement `append_to_entity(df, entity_metadata)` — same as above with
  `mode("append")`.
- [ ] Extract a shared `_build_write_options(entity_metadata)` helper to avoid duplication
  between the two methods (and future streaming write).
- [ ] Update class docstring.

Files: `packages/kindling/entity_provider_csv.py`

### 2) Unit tests

- [ ] `test_csv_write_to_entity_overwrites` — verify existing files are replaced.
- [ ] `test_csv_append_to_entity_preserves_existing` — verify rows accumulate.
- [ ] `test_csv_write_respects_options` — delimiter, header, compression round-trip.
- [ ] `test_csv_is_writable` — assert `is_writable(provider)` returns `True`.

Files: `tests/unit/test_csv_entity_provider.py` (create if absent)

### 3) Streaming write (follow-up)

- [ ] Evaluate use cases before implementing.
- [ ] If pursued: add `StreamWritableEntityProvider`, implement `append_as_stream()` using
  `df.writeStream.format("csv").option("path", ...).option("checkpointLocation", ...).start()`.
- [ ] Add streaming write tests.

Files: `packages/kindling/entity_provider_csv.py`, `tests/unit/test_csv_entity_provider.py`

### 4) Docs update

- [ ] Add write usage example to provider reference docs.
- [ ] Note `compression` option and part-file output behavior (Spark writes a directory of
  part-files, not a single `.csv`).

Files: `docs/reference/` (whichever file covers entity providers)

### 5) Regression tests

- [ ] `pytest -q tests/unit/test_csv_entity_provider.py`
- [ ] `pytest -q tests/unit/` (full unit suite — confirm no provider registry regressions)

## Acceptance Criteria

1. `is_writable(CSVEntityProvider(...))` returns `True`.
2. `write_to_entity` produces a readable CSV directory that round-trips through `read_entity`.
3. `append_to_entity` accumulates rows across calls.
4. Write options (`delimiter`, `header`, `compression`) are honoured.
5. All existing CSV read tests continue to pass.
