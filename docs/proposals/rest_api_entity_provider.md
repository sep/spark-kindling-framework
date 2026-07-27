# REST API Entity Provider

**Status:** Proposed
**Scope:** A read-only, strongly typed REST API entity provider with cursor-based incremental reads
**Target provider type:** `rest-api`

## Summary

Add a `rest-api` entity provider that exposes records from an HTTP API as
Kindling entities. The provider uses an OpenAPI document to describe the
operation, request parameters, response shape, pagination, and authentication
requirements. It converts successful JSON responses into Spark DataFrames using
the entity's declared schema and implements
`IncrementalReadableEntityProvider` so watermarked pipes can extract only new or
changed records.

The first implementation should be a batch, read-only source. It should not
pretend that a REST API is a Spark streaming source, and it should not infer
business semantics from an OpenAPI document alone. OpenAPI supplies transport
and wire-format metadata; the Kindling entity declaration remains the durable
data contract.

## Why this belongs in the provider layer

An API-backed source is currently possible through a custom pipe, but that puts
HTTP concerns in application code:

- authentication and secret lookup;
- pagination and rate-limit handling;
- response validation and JSON-to-Spark conversion;
- retries and request telemetry;
- incremental query parameters and cursor advancement; and
- replay behavior after a downstream failure.

Those concerns are provider behavior. Keeping them in a provider lets ordinary
pipes remain pure DataFrame transformations and lets the existing watermark
aspect own the commit point for source progress.

The provider is also a useful boundary for APIs that are too small or too
irregular to justify a native Spark connector. It is not intended to replace
Delta for internal storage or to move unbounded API volumes through the driver.

## Goals

1. Read paginated REST resources into a DataFrame with a declared Spark schema.
2. Use OpenAPI as the source of truth for operation and response typing without
   performing network work during module import or entity registration.
3. Support incremental extraction through the existing opaque cursor contract.
4. Make retries, pagination, rate limits, authentication, and response errors
   deterministic and observable.
5. Preserve at-least-once delivery semantics and make replay safe when the
   downstream entity uses an appropriate business key and merge mode.
6. Validate configuration and unsupported API shapes before making an API call.
7. Keep provider-specific configuration in `provider.*` tags and framework-wide
   defaults in `kindling.rest_api.*` configuration.

## Non-goals for the first version

- Writing records back to the API.
- Treating polling as a continuous Spark streaming source.
- Inferring a complete business schema from arbitrary JSON at runtime.
- Guaranteeing exactly-once effects against an API that has no snapshot or
  change-feed semantics.
- Deriving deletions from the absence of a record in an ordinary list response.
- Supporting every OpenAPI construct, pagination convention, or authentication
  scheme on day one.
- Hiding a large extraction behind an unbounded driver-side collection.

## Existing Kindling contracts

The provider should compose the existing interfaces rather than introduce a
provider-specific execution path:

| Interface | REST provider position |
|---|---|
| `BaseEntityProvider` | Required; batch read and existence check |
| `IncrementalReadableEntityProvider` | Required for watermarked extraction |
| `WritableEntityProvider` | Not implemented initially; API is a source |
| `StreamableEntityProvider` | Not implemented initially |
| `DestinationEnsuringProvider` | Not implemented; the API is not a Kindling-managed destination |

The entity provider registry would register `rest-api` as an optional built-in
only if its HTTP/OpenAPI dependencies are available, or through a small
extension if dependency isolation is preferred. Missing optional dependencies
must produce an actionable installation error when the provider is selected,
not when Kindling imports unrelated providers.

The incremental interface already defines the essential semantics:

- the cursor is an opaque provider-defined string;
- `cursor=None` means initial load;
- `(None, None)` means no new data;
- the returned cursor must cover exactly the returned DataFrame; and
- the cursor is persisted only after the output is durably persisted.

The REST provider should follow those rules exactly. It must not make the
watermark manager understand timestamps, page tokens, or API-specific cursors.

## Proposed architecture

```text
entity declaration + provider tags
             |
             v
     RestApiEntityProvider
       |       |       |
       |       |       +--> cursor codec / incremental policy
       |       +----------> OpenAPI operation + response validator
       +------------------> HTTP client + pagination + auth
             |
             v
       typed Python records
             |
             v
      Spark DataFrame
             |
             v
       ordinary Kindling pipe
             |
             v
   downstream Delta/other target
```

The provider should be split into testable components:

1. **OpenAPI loader/resolver** — loads a local or governed file, resolves
   `$ref`, selects an operation, and produces a normalized operation model.
2. **Schema mapper** — maps the selected response schema to Spark `DataType`
   values and validates it against `EntityMetadata.schema`.
3. **Request planner** — renders path, query, header, and body parameters from
   the entity configuration and incremental cursor.
4. **HTTP client** — applies authentication, timeouts, retries, rate-limit
   handling, and redacted telemetry.
5. **Paginator** — follows the configured pagination strategy and emits decoded
   records without exposing page mechanics to the runner.
6. **Cursor policy** — interprets the opaque stored cursor, determines the
   request lower bound, computes the high-water cursor from returned records,
   and applies overlap/tie-breaking rules.
7. **Record materializer** — validates/coerces each response record and creates
   a DataFrame using the declared schema.

The provider may materialize one bounded extraction in Python before creating a
DataFrame, as the API-based ADX provider does, but it must enforce configurable
record, byte, and page limits. Large extracts should be partitioned into
bounded runs or use a source-specific bulk export rather than silently
accumulate in driver memory.

## OpenAPI as the typing and request contract

### Spec resolution

The provider should accept a local path or a governed URI for the OpenAPI
document. A remote spec must be fetched at read time through the same controlled
HTTP/auth policy or staged as an artifact; it must not be fetched while a module
is imported. A deployment may pin a spec digest or version so a run is
reproducible.

Suggested tags:

```yaml
provider_type: rest-api
provider.openapi_spec: /Volumes/<catalog>/<schema>/<volume>/api/openapi.yaml
provider.openapi_operation_id: listRecords
provider.base_url: https://api.example.invalid
```

`provider.openapi_operation_id` is preferred over a path/method pair because it
is stable across harmless path changes and is already a first-class OpenAPI
concept. The provider should reject an operation whose response has no
successful JSON schema unless an explicit raw-response mode is introduced
later.

### Entity schema remains authoritative

OpenAPI should not cause a network call or an implicit schema mutation during
entity registration. The recommended contract is:

1. the application declares an explicit Spark schema on the entity;
2. the provider resolves the selected OpenAPI response schema at read time;
3. the provider validates that the response can be represented by the declared
   schema; and
4. the provider materializes records using that declared schema.

This keeps entity metadata stable across environments and makes empty results
typed. It also avoids Spark's permissive and inconsistent inference for empty,
nullable, or mixed JSON values.

An optional offline generation tool may later generate a Spark schema skeleton
from OpenAPI, but the generated schema should be checked into the application
and reviewed like any other contract. Runtime schema inference should not be the
default.

### Initial type mapping

| OpenAPI shape | Spark type | Initial policy |
|---|---|---|
| `string` | `StringType` | Preserve text; validate `format` where configured |
| `string` + `date` | `DateType` | Parse ISO date; fail malformed values |
| `string` + `date-time` | `TimestampType` | Normalize to UTC; define behavior for missing offsets |
| `integer` `int32`/`int64` | `IntegerType`/`LongType` | Reject overflow |
| `number` `float`/`double` | `FloatType`/`DoubleType` | Reject non-finite values unless explicitly allowed |
| `boolean` | `BooleanType` | Do not coerce arbitrary strings by default |
| `array` | `ArrayType` | Recursively map item schema |
| object with properties | `StructType` | Recursively map named properties |
| object with `additionalProperties` | `MapType` or `StringType` | Require an explicit provider policy for dynamic objects |
| `nullable` / `type: [T, null]` | nullable field | Preserve null; do not invent defaults |
| `enum` | base scalar type | Validate values; enum enforcement may be warn/fail configured |

`oneOf`, `anyOf`, polymorphic discriminators, recursive references, and
unconstrained objects require explicit support. Until then, registration or
startup validation should fail with the operation and field that is unsupported.
Silently converting a polymorphic object to a string would violate the strong
typing goal.

Unknown response fields should be ignored only when the entity explicitly opts
into that behavior. The default should be strict enough to detect API contract
drift, with a configurable `fail`, `warn`, or `ignore` policy analogous to
Kindling's schema-drift policy. Missing fields, nullability changes, and type
changes must be reported with JSON paths.

## Incremental extraction model

### Provider-owned cursor

The provider should persist a versioned JSON cursor as the opaque string. The
watermark layer stores it without inspecting it. A timestamp-only cursor is not
safe for APIs where multiple records share a timestamp, so the cursor should be
able to contain a high-water tuple and policy metadata, for example:

```json
{
  "version": 1,
  "position": {
    "updated_at": "2026-01-01T00:00:00Z",
    "id": "record-00042"
  },
  "spec_digest": "sha256:..."
}
```

The exact JSON fields are illustrative. The provider owns the format and must
be able to reject cursors it cannot safely interpret. The cursor should not
contain a transient page token because page tokens commonly expire and are only
valid within one extraction. Pagination must finish before the new high-water
cursor is returned.

### Supported incremental strategies

The first version should support APIs that expose a stable, ordered change
field through a query parameter:

```yaml
provider.incremental.field: updatedAt
provider.incremental.request_parameter: updatedAfter
provider.incremental.order_by: updatedAt,id
provider.incremental.lookback: PT5M
provider.incremental.initial_value: "1970-01-01T00:00:00Z"
```

The provider should model these strategies explicitly:

| Strategy | Use |
|---|---|
| `timestamp` | Filter by a server-side `updated_at`/`modified_at` value |
| `timestamp_key` | Filter by `(timestamp, stable key)` to handle collisions |
| `opaque_api_cursor` | Pass a server-issued continuation/change-feed cursor |
| `snapshot_window` | Re-read a bounded overlap window and merge downstream |

`timestamp_key` is preferred when the API can order by both fields. If the API
only supports a timestamp filter, the provider should use an inclusive boundary
or configured lookback window and rely on an idempotent downstream merge. The
provider must document whether the lower bound is exclusive, inclusive, or
overlapped.

### Read lifecycle

For a watermarked read:

1. Read the stored opaque cursor for `(source entity, pipe)`.
2. Translate it into the API's incremental request parameters.
3. Establish a bounded upper position when the API supports one. For example,
   capture an API snapshot token or the current UTC high-water time before the
   first page.
4. Request pages in deterministic order, following only validated next links or
   configured page parameters.
5. Validate and materialize all records returned for the extraction.
6. Compute a new cursor that covers exactly those records.
7. Return `(DataFrame, new_cursor)` to the watermark aspect.
8. Let the existing lifecycle advance the stored cursor only after the output
   persist succeeds.

On any request, validation, conversion, or downstream failure, the previous
cursor must remain unchanged. The next run replays the same slice. The provider
must therefore tolerate repeated requests and downstream entities should use
stable merge keys where the API can return duplicates from overlap windows.

### Deletes and mutable records

An ordinary `GET /records` endpoint cannot distinguish “deleted” from “not in
this page.” The provider must not manufacture delete events. Deletion support
requires one of:

- a change-feed endpoint with explicit delete records;
- a tombstone field in the response;
- a documented delete endpoint/query that can be polled incrementally; or
- an explicitly configured snapshot reconciliation mode.

The first version should support tombstones or change-feed records only if they
are represented in the declared entity schema. Snapshot reconciliation should
be a separate capability because it needs a bounded, complete snapshot and a
policy for records absent from it.

## Pagination and consistency

Pagination is part of the operation contract, not application pipe logic. The
provider should support a small set of explicit strategies:

- `next_link`: response contains an absolute or relative link to the next page;
- `page_offset`: configured page and page-size parameters;
- `page_number`: configured page-number parameter; and
- `cursor`: request and response fields carry a continuation token.

The provider must enforce a maximum page count, maximum records, maximum bytes,
and a no-progress guard. It must reject a repeated next link or cursor rather
than loop indefinitely. Absolute next links should be restricted to the
configured host unless an explicit cross-host policy permits otherwise.

If the API offers snapshot or consistency tokens, the provider should use them
for all pages. If it does not, the provider should expose the weaker guarantee:
records may be added, updated, or deleted while pages are being read. A
lookback window and downstream merge reduce missed updates but do not create a
true snapshot. The proposal should not claim stronger semantics than the API
provides.

## Authentication and security

Supported authentication should be deliberately small and use platform secret
resolution:

- API key in a configured header;
- bearer token from a secret reference; and
- OAuth2 client credentials, with client ID and secret references.

Mutual TLS and signed requests can be added as separate auth plugins. Secret
values must never be stored in entity tags, logged, included in exception text,
or serialized into cursors. Configuration should refer to secret identifiers or
scopes, for example:

```yaml
provider.auth.mode: bearer
provider.auth.secret_scope: <secret-scope>
provider.auth.secret_key: <secret-key>
```

The HTTP client must redact configured authorization headers, API keys, query
parameters, and secret references from request logs. OpenAPI documents may also
contain server URLs or example values, so spec access should follow the same
governance rules as other deployment artifacts.

## Configuration surface

### Entity-level tags

The entity selects the operation and declares the source-specific contract:

```python
@DataEntities.entity(
    entityid="source.records",
    name="Source records",
    merge_columns=["record_id"],
    schema=RECORD_SCHEMA,
    tags={
        "provider_type": "rest-api",
        "provider.openapi_spec": "config/api.yaml",
        "provider.openapi_operation_id": "listRecords",
        "provider.base_url": "https://api.example.invalid",
        "provider.pagination.strategy": "next_link",
        "provider.incremental.strategy": "timestamp_key",
        "provider.incremental.field": "updatedAt",
        "provider.incremental.request_parameter": "updatedAfter",
        "provider.incremental.order_by": "updatedAt,record_id",
        "provider.incremental.lookback": "PT5M",
        "provider.auth.mode": "bearer",
        "provider.auth.secret_scope": "<secret-scope>",
        "provider.auth.secret_key": "<secret-key>",
    },
)
def source_records():
    pass
```

The exact tag names should be finalized with the implementation. They should
remain flat and provider-scoped so they work with the existing entity tag
override mechanism. A deployment can change the base URL, spec location, or
secret references without creating a second logical entity.

### Framework-level defaults

The following settings are candidates for `kindling.rest_api.*` YAML keys:

```yaml
kindling:
  rest_api:
    timeout_seconds: 30
    connect_timeout_seconds: 10
    max_retries: 4
    backoff_seconds: 1
    max_pages: 10000
    max_records: 1000000
    max_response_bytes: 268435456
    response_drift: fail
    user_agent: kindling-rest-api
```

Entity tags override defaults for an individual source. Operational limits
should have safe finite defaults; “unlimited” should require an explicit opt-in.

## Error handling and observability

Errors should be classified so retries do not amplify permanent failures:

| Class | Examples | Action |
|---|---|---|
| Authentication | 401, invalid secret, expired token | Fail; do not retry blindly |
| Authorization | 403 | Fail and identify operation/resource without secrets |
| Not found/configuration | 404 for operation or spec | Fail; likely configuration error |
| Rate limit | 429, `Retry-After` | Retry within bounded policy |
| Transient server | 408, 5xx, connection reset | Retry idempotent GETs with backoff |
| Contract error | invalid JSON, schema mismatch | Fail; preserve cursor |
| Pagination safety | repeated token, limit exceeded | Fail; preserve cursor |
| Empty result | valid zero-record page/extraction | Return typed empty or no-new-data according to cursor policy |

Emit request and extraction metrics without logging payloads by default:

- entity ID, operation ID, and spec digest;
- request count, page count, record count, and response bytes;
- latency and retry count;
- HTTP status class and rate-limit wait time;
- old and new cursor fingerprints, never raw credentials; and
- schema validation warnings or failures.

The provider should include a correlation/request ID when the API supports it
and propagate a run or persist identifier through headers only when explicitly
configured. Request bodies and response records should not be emitted to normal
logs.

## Strong typing and schema drift

The provider should distinguish three validation layers:

1. **OpenAPI operation validation** — the selected operation has a supported
   successful response, pagination metadata, and configured incremental fields.
2. **Payload validation** — each response page is valid JSON and conforms to the
   selected response schema sufficiently to materialize records.
3. **Entity contract validation** — the materialized fields and types satisfy
   the declared Spark schema and merge-key requirements.

Schema drift behavior should be explicit:

- `fail`: stop before returning a DataFrame;
- `warn`: emit a quality signal and materialize only safe fields; or
- `ignore`: tolerate unknown fields but still reject missing required fields and
  unsafe type changes.

`warn` and `ignore` must not silently invent values for missing fields. A
schema-drift event should identify the OpenAPI/spec digest, response JSON path,
expected type, observed type, and entity ID.

## Example application shape

The API entity is a source; a pipe owns business transformations and the Delta
entity owns durable storage:

```python
@DataEntities.entity(
    entityid="bronze.records",
    name="API records",
    merge_columns=["record_id"],
    schema=RECORD_SCHEMA,
    tags={
        "provider_type": "rest-api",
        "provider.openapi_spec": "config/api.yaml",
        "provider.openapi_operation_id": "listRecords",
        "provider.incremental.strategy": "timestamp_key",
        "provider.incremental.field": "updatedAt",
        "provider.incremental.request_parameter": "updatedAfter",
        "provider.incremental.order_by": "updatedAt,record_id",
    },
)
def api_records():
    pass


@DataEntities.entity(
    entityid="silver.records",
    name="Clean records",
    merge_columns=["record_id"],
    schema=CLEAN_RECORD_SCHEMA,
    tags={"provider_type": "delta", "write.mode": "merge"},
)
def clean_records():
    pass


@DataPipes.pipe(
    pipeid="silver.clean_records",
    name="Clean API records",
    input_entity_ids=["bronze.records"],
    output_entity_id="silver.records",
    output_type="delta",
    use_watermark=True,
)
def clean_api_records(bronze_records: DataFrame) -> DataFrame:
    return bronze_records.filter("record_id IS NOT NULL")
```

The first input remains the driving input, so only the REST source is read
incrementally. The pipe must not call the API directly, save the output, or
advance a cursor itself.

## Correctness requirements

The implementation is acceptable only if it can demonstrate the following:

- Initial load returns a typed DataFrame and a cursor that covers the complete
  returned extraction.
- A subsequent read requests records after the stored cursor and does not
  re-read the entire source unless configured to use an overlap window.
- Records sharing the same timestamp are not skipped.
- A cursor is not advanced when a page, conversion, transform, or persist fails.
- Re-running a failed extraction is safe and produces the same logical result.
- A late-arriving update is covered by a documented overlap/change-feed policy.
- Pagination ends deterministically and cannot loop on a repeated token/link.
- Empty pages and empty extractions preserve the declared schema.
- Deletes are represented only when the source provides explicit delete
  semantics or snapshot reconciliation is explicitly enabled.
- Concurrent executions of the same `(source entity, pipe)` remain unsupported,
  consistent with the current watermark aspect contract.

## Testing strategy

### Unit tests

- OpenAPI `$ref`, response selection, operation validation, and spec digest.
- OpenAPI-to-Spark mapping for scalars, nullable fields, arrays, nested
  structs, maps, enums, and unsupported unions.
- Path/query/header rendering with cursor and page parameters.
- Cursor encoding/decoding, version rejection, timestamp collisions, and
  lookback windows.
- `next_link`, offset, page-number, and API-cursor pagination.
- Retry classification, `Retry-After`, timeout, maximum limits, and no-progress
  guards.
- Secret/header redaction and absence of credentials in errors or cursors.
- Empty responses, malformed payloads, unknown fields, missing fields, and
  type drift.

### Provider integration tests

Use a local mock HTTP server and a real Spark session to verify:

1. initial extraction across multiple pages;
2. incremental extraction across two runs;
3. same-timestamp records with deterministic tie-breaking;
4. an inclusive overlap window with downstream merge deduplication;
5. a failure after page N followed by a replay;
6. a 429 response followed by a successful retry;
7. an empty typed result; and
8. explicit tombstones or a documented rejection of delete configuration.

### End-to-end watermark tests

Run a watermarked pipe with a Delta target and assert that:

- the source cursor is written only after persist succeeds;
- a failed persist leaves the old cursor unchanged;
- the next run replays the failed source slice; and
- a no-new-data result does not invoke the downstream transform or advance the
  cursor incorrectly.

These tests should use the existing watermark manager and lifecycle signals,
not a second test-only cursor implementation.

## Delivery plan

### Phase 1 — Contract and local provider

- Add the provider interfaces/adapters and register `rest-api`.
- Support a local OpenAPI 3 document, one `GET` operation, JSON object/list
  responses, explicit Spark schema, and `next_link` or offset pagination.
- Implement a timestamp-based incremental strategy with an opaque JSON cursor.
- Add mock-server and watermark integration tests.

### Phase 2 — Production transport behavior

- Add bearer/API-key/OAuth2 secret resolution.
- Add bounded retries, rate-limit handling, request limits, redacted metrics,
  and spec digests.
- Add deterministic timestamp-plus-key ordering and configurable overlap.
- Add configuration reference and operational troubleshooting guidance.

### Phase 3 — API diversity and change semantics

- Add server-issued change-feed cursors.
- Add explicit tombstone support and, separately, snapshot reconciliation.
- Add more OpenAPI composition support (`allOf`, discriminators, selected
  `oneOf` patterns) only with clear Spark mappings.
- Consider an offline schema-generation command and contract artifact checks.

## Alternatives considered

### Custom pipe using `requests`

This is suitable for a one-off endpoint but duplicates auth, pagination,
typing, retry, and watermark behavior across applications. It remains a useful
escape hatch for APIs that do not fit the provider's supported contract.

### Generate a typed SDK from OpenAPI and call it in application code

SDK generation improves client ergonomics but does not solve cursor persistence,
Spark schema materialization, pagination limits, or failure-safe watermark
advancement. A generated SDK may become an implementation detail of the
provider later.

### Use a generic HTTP/Spark connector

A connector may provide parallelism or streaming primitives, but often cannot
express API-specific pagination and high-water semantics. The provider can
adopt a connector internally if it preserves the same Kindling contracts.

### Continuous polling as a streaming source

This would require a durable source offset and API-specific recovery semantics
that differ from Spark file/event sources. Batch polling with `use_watermark`
is easier to reason about and provides a clear first implementation. A future
stream adapter should be a separate capability rather than making batch reads
pretend to be streams.

## Open questions

1. Should the provider be a built-in optional dependency or a separately
   installable extension?
2. Which OpenAPI versions and JSON Schema dialects are required initially?
3. Should schema validation use a third-party JSON Schema validator, generated
   Python models, or a small normalized validator owned by the provider?
4. Which authentication mechanisms are available through the platform's secret
   provider in every supported deployment environment?
5. Should the framework add a first-class typed cursor codec, or is a provider
   owned versioned string sufficient for REST and future cursor providers?
6. What maximum response size and record count are safe defaults for the
   supported Spark runtimes?
7. Do we need a standard provenance column set, or should source request/cursor
   metadata remain in signals and run telemetry?

## Recommendation

Proceed with a read-only `rest-api` provider implementing
`BaseEntityProvider` and `IncrementalReadableEntityProvider`. Require an
explicit entity schema, use a locally resolved or pinned OpenAPI document for
operation and response validation, and start with timestamp-plus-key
incrementality, deterministic pagination, bounded retries, and at-least-once
replay semantics. Defer writes, continuous streaming, and broad polymorphic
JSON support until the batch contract has been proven with real APIs and
watermark failure tests.
