# Unified OTLP Telemetry Provider (Azure Monitor + Databricks Zerobus)

**Status:** Proposed. No framework or platform code changes made yet.
**Created:** 2026-08-27
**Scope:** Replace the Application-Insights-Distro-based `kindling_ext_otel_azure`
with a single, backend-agnostic extension implementing Kindling's existing
`SparkTraceProvider` and `PythonLoggerProvider`/`SparkLoggerProvider` on top
of the standard OpenTelemetry SDK's OTLP exporters, with two pluggable
destinations at launch: Azure Monitor's native OTLP ingestion and Databricks
Zerobus Ingest's native OTLP endpoint. Metrics are out of scope for the first
release; Fabric/Synapse/standalone Spark get whatever Azure Monitor's OTLP
path already covers them, with no platform-specific code of their own.
**Related:** `contributing/logging_tracing.md` (span/log provider contracts
this extension must honor), `iceberg_entity_provider.md` (precedent for
naming preview-status platform risk explicitly rather than glossing over
it).
**Supersedes:** the original two-extension framing of this proposal
(`kindling_ext_otel_azure` Distro-based + a new Databricks-only
`kindling_ext_otel_databricks`). Both destinations turned out to speak the
same protocol with the same shape of problem (per-signal endpoint + bearer
token + Collector-recommended token refresh), so one extension covers both.

## Accepted risk

Both destinations are **preview/beta on their own platforms** as of this
writing:

- Azure Monitor's native OTLP ingestion is explicitly documented as preview,
  with the Supplemental Terms of Use for Azure previews applying. Source:
  [OpenTelemetry ingestion options
  (preview)](https://learn.microsoft.com/en-us/azure/azure-monitor/containers/opentelemetry-summary).
- Zerobus's OTLP endpoint status is inconsistent across Databricks' own docs
  and third-party write-ups — the general Zerobus overview groups it with
  GA features, while a practitioner reference calls it Beta and not yet
  billed. Sources: [Zerobus Ingest
  overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview),
  [otel-zerobus](https://github.com/Booleans/otel-zerobus).

**Decision: proceed anyway.** This is a deliberate choice, not an oversight —
both vendors are converging their telemetry ingestion on plain OTLP, and
building against that shape now is the right long-term bet even while the
specific endpoints are still labeled preview. Phase 0 (below) exists
specifically to re-confirm current status and catch breaking changes before
committing to Phase 1 implementation.

## Decision

Replace `kindling_ext_otel_azure`'s `configure_azure_monitor()`-based
implementation with one new extension built on the standard
`opentelemetry-exporter-otlp-proto-grpc`/`-http` packages, structured as:

- **Shared, backend-agnostic provider classes** — `OtlpTraceProvider` and
  `OtlpLoggerProvider`, implementing `SparkTraceProvider` and
  `PythonLoggerProvider`/`SparkLoggerProvider` exactly the way
  `AzureMonitorTraceProvider`/`AzureMonitorLoggerProvider` do today (same
  `span()`/`start_span()`/`add_event()`/`end_span()`/`record_span()` seam,
  same attribute-coercion and trace-context-injection behavior). These
  classes call nothing vendor-specific; they only need a `TracerProvider`/
  `LoggerProvider` that's already been wired to *some* OTLP exporter.
- **A pluggable `OtlpDestination`** — per-signal endpoint URLs (traces, logs;
  metrics endpoint plumbed but unused, see below) plus an `OtlpTokenProvider`
  that mints and refreshes the bearer token each destination requires.
- **Two `OtlpTokenProvider` implementations at launch**:
  - `EntraIdTokenProvider` — Azure Monitor native OTLP, via managed identity
    or service-principal client-credentials, scope
    `https://monitor.azure.com/.default`.
  - `DatabricksOAuthTokenProvider` — Zerobus OTLP, via Databricks
    service-principal client-credentials, token downscoped per destination
    table via `authorization_details`.
- **Explicit backend selection via config**, not platform auto-detection —
  which destination to use is a user choice (an Azure-hosted Databricks shop
  might prefer either), the same way an entity's `provider_type` tag is an
  explicit choice rather than an inferred one.

This is a strictly smaller amount of backend-specific code than the current
`kindling_ext_otel_azure`: no vendor distro package, no connection-string
parsing, no `azure-monitor-opentelemetry` dependency. Both backends reduce to
"which token provider, which endpoints."

## Why the two destinations turned out to be one problem

Investigating Zerobus's OTLP endpoint and then re-checking Azure Monitor's
current OTLP story (Azure has moved since `kindling_ext_otel_azure` was
built) surfaced the same shape on both sides:

| | Azure Monitor native OTLP | Zerobus OTLP |
|---|---|---|
| Endpoint shape | Per-signal HTTPS URL, `.../otlp/v1/{traces,logs,metrics}` via a Data Collection Rule/Endpoint | Per-signal HTTPS/gRPC endpoint, one Unity Catalog table per signal |
| Wire format | Standard OTLP | Standard OTLP |
| Auth | Microsoft Entra ID bearer token (managed identity or service principal, scope `https://monitor.azure.com/.default`) | Databricks OAuth bearer token (service principal, downscoped per table via `authorization_details`) |
| Extra header | none beyond the bearer token | `x-databricks-zerobus-table-name: <catalog>.<schema>.<table>` |
| Token lifetime | short-lived, Entra ID default | ~1 hour |
| Vendor's recommended token-lifecycle pattern | OTel Collector + `azure_auth` extension | OTel Collector + `oauth2clientauthextension` |
| Table/resource provisioning | DCR/DCE + workspaces provisioned via ARM template or the Application Insights "OTLP support" toggle | Unity Catalog tables must be pre-created with an exact schema; never auto-created or evolved |
| Status | Preview | Beta / status inconsistent across sources |

Sources: [Ingest OTLP data into Azure Monitor with OTel
Collector](https://learn.microsoft.com/en-us/azure/azure-monitor/containers/opentelemetry-protocol-ingestion),
[Configure OpenTelemetry (OTLP) clients to send data to Unity
Catalog](https://docs.databricks.com/aws/en/ingestion/opentelemetry/configure),
[OpenTelemetry table
reference](https://docs.databricks.com/aws/en/ingestion/opentelemetry/table-reference).

Given that, building two separate provider implementations (one wrapping a
vendor distro, one wrapping raw OTLP) would have meant duplicating the entire
span/log adapter layer for no reason — the only thing that legitimately
differs is "how do I get a valid bearer token for this specific
destination," which is exactly the shape an interface (`OtlpTokenProvider`)
should carry.

## Key design decisions

### 1. In-process token refresh, Collector documented as an alternative

Both vendors recommend an OTel Collector as the production pattern for token
lifecycle (Azure's `azure_auth` extension, Databricks'
`oauth2clientauthextension`). That fits a long-running service; it fits
Kindling's job model poorly, since Kindling apps run as `.kda` archives on
Spark drivers with no standing sidecar.

Recommendation: each `OtlpTokenProvider` owns its own cache-and-refresh
logic in-process (refresh at ~80% of TTL), the same way `AzureMonitorConfig`
owns SDK lifecycle today.

- gRPC exporters: wrap `grpc.ChannelCredentials` with a
  `grpc.AuthMetadataPlugin` backed by the active `OtlpTokenProvider`, so
  token refresh happens per-call without rebuilding the channel.
- HTTP/protobuf exporters have no per-call auth hook; if used, the extension
  must periodically rebuild the exporter/processor pipeline on a timer
  instead. Prefer gRPC unless a Phase 0 spike finds a blocking compatibility
  issue in a Databricks or Fabric/Synapse cluster's Python environment.
- A Collector remains a documented, supported alternative for teams that
  already run one — the extension just doesn't require it.

### 2. Traces and logs only; metrics deferred

Kindling core has `SparkTraceProvider` (spans) and `PythonLoggerProvider`
(logs), but no metrics/meter abstraction — adding one is a separate,
cross-cutting decision out of scope here. Both vendors also add real
friction on the metrics path specifically (Azure Monitor's OTLP metrics
require delta temporality and exponential-histogram aggregation, needing a
`cumulativetodelta` processor for SDKs that default to cumulative; Zerobus
flags integer-precision loss above 2^53 and unsigned-field overflow), which
reinforces deferring it rather than rushing a third signal into v1.

### 3. Table/resource provisioning stays a one-time, out-of-band step

Neither destination supports runtime auto-provisioning that's safe to run
from a job's driver: Zerobus never auto-creates or evolves its target
tables, and Azure Monitor's DCR/DCE/workspace setup is an ARM-template or
portal-driven one-time action, not something to trigger per job start. Ship
the extension with documented one-time setup steps per backend (DDL for
Zerobus tables, ARM template or the Application Insights "OTLP support"
toggle for Azure) rather than folding either into `kindling migrate` — the
schema/resources are dictated entirely by the vendor, not declared by the
app, so there's nothing for desired-state convergence to do.

The exact Zerobus `CREATE TABLE` DDL was not fully recoverable from the
documentation fetched during this exploration; **pulling the authoritative
DDL, and confirming the current Application Insights OTLP-toggle flow, are
both Phase 0 tasks**, not assumptions safe to build against yet.

### 4. Explicit backend selection, not platform auto-detection

Backend choice is a config value, never inferred from
`kindling.initialize()`'s detected platform. An Azure-hosted Databricks
deployment might reasonably want either destination; forcing a choice by
platform would remove that and, unlike genuine platform facts (you're on
exactly one platform), telemetry backend is a preference, not a fact to
detect.

## Migration from `kindling_ext_otel_azure`

This is a breaking change for existing users of the Distro-based extension,
and needs to be treated as one:

- **Package**: `spark-kindling-ext-otel-azure` → a new package name (open
  question below); the old package should be marked deprecated in its
  `CHANGELOG.md` and `README.md`, pointing at the replacement, per one more
  release before removal.
- **Config**: `kindling.telemetry.azure_monitor.connection_string`
  (instrumentation-key auth) has no equivalent in the native-OTLP world —
  Entra ID bearer tokens replace it entirely. Existing app configs using a
  connection string will not work unchanged; document the new
  `client_id`/`client_secret` (or managed-identity) shape explicitly as a
  migration step, not an automatic fallback.
- **Behavior**: Application Insights' Live Metrics and some Distro-only
  auto-instrumentation conveniences are not part of native OTLP ingestion.
  Call this out in the migration note so teams relying on Live Metrics don't
  discover the gap after upgrading.
- Existing `kindling_ext_otel_azure` installations keep working until they
  choose to migrate — this proposal does not require an in-place breaking
  upgrade of that package, only that it stops being the recommended path for
  new work once the replacement ships.

## Configuration contract

```yaml
kindling:
  extensions:
    - spark-kindling-ext-otel>=0.1.0   # name TBD, see open questions
  telemetry:
    otlp:
      backend: azure_monitor   # or: databricks_zerobus
      enable_logging: true
      enable_tracing: true

      azure_monitor:
        # Data Collection Endpoint / Rule values from the Application
        # Insights "OTLP Connection Info" panel, or manual ARM provisioning.
        traces_endpoint: "https://<logs-dce-domain>/dataCollectionRules/<dcr-id>/streams/Microsoft-OTLP-Traces/otlp/v1/traces"
        logs_endpoint: "https://<logs-dce-domain>/dataCollectionRules/<dcr-id>/streams/Microsoft-OTLP-Logs/otlp/v1/logs"
        # Managed identity is preferred; client_id/secret is the fallback
        # for non-Azure-hosted compute.
        client_id: "@secret:otel_azure_client_id"
        client_secret: "@secret:otel_azure_client_secret"

      databricks_zerobus:
        workspace_url: "my-workspace.cloud.databricks.com"
        region: "us-west-2"
        catalog: "observability"
        schema: "otel"
        table_prefix: "myapp"
        client_id: "@secret:otel_databricks_client_id"
        client_secret: "@secret:otel_databricks_client_secret"
```

Only the block matching `backend` is read; the other is inert. Absence of
required credentials for the selected backend, or both enable flags false,
must no-op exactly like `AzureMonitorConfig.initialize` does today — never
raise during bootstrap.

## Provider design

- `OtlpConfig` (parallels `AzureMonitorConfig`): reads `backend`, resolves
  the matching `OtlpTokenProvider` and endpoint set, builds the shared
  `TracerProvider`/`LoggerProvider` with a `BatchSpanProcessor`/
  `BatchLogRecordProcessor` wrapping the appropriate OTLP exporter, and owns
  `is_configured()`/`force_flush()`/`shutdown()`.
- `OtlpTraceProvider(SparkTraceProvider)` / `OtlpLoggerProvider` — copy of
  `AzureMonitorTraceProvider`/`AzureMonitorLoggerProvider`'s bodies with the
  Azure-only references removed; they only ever call generic
  `opentelemetry.trace`/`logging` APIs.
- `OtlpTokenProvider` (ABC): `get_token(destination) -> str`, internally
  cached and refreshed ahead of expiry. `EntraIdTokenProvider` and
  `DatabricksOAuthTokenProvider` are the two concrete implementations;
  adding a third backend later means implementing this one interface, not
  touching the trace/log provider classes at all.
- `_register_providers()` at module import time rebinds
  `PythonLoggerProvider`, `SparkLoggerProvider`, and `SparkTraceProvider` to
  the OTLP-backed singletons — same registration mechanics as
  `kindling_ext_otel_azure/__init__.py` today.

No changes to `kindling/spark_trace.py`, `trace_ops.py`, or any call site
already using `trace_provider.span(...)` — this is a new provider
implementation behind the existing seam. Kindling's existing span-volume
discipline (tiered `minimal`/`standard`/`verbose` levels, no per-row spans,
whitelisted attributes only) already caps request volume against either
backend's quota; this should be confirmed against real numbers in Phase 0,
not assumed.

## Packaging and runtime requirements

Extension-only, never core. Dependencies: `opentelemetry-api`/`-sdk` (same
line `kindling_ext_otel_azure` already pins, `^1.20.0`, for compatibility if
both packages are ever installed side-by-side during migration) plus
`opentelemetry-exporter-otlp-proto-grpc` (or `-http`). No vendor distro
package (`azure-monitor-opentelemetry`) required at all. Phase 0 must check
these against whatever OTel packages Databricks Runtime and Fabric/Synapse
bundle, the same way Fabric's pinned `azure-core` forced the current
extension's dependency line.

## Validation plan

### Unit tests

- Token cache per destination; refresh triggered before TTL expiry; no
  network call when a cached token is valid.
- `span()`/`start_span()`/`end_span()` attribute mapping matches existing
  `AzureMonitorTraceProvider` test coverage (non-primitive detail coercion,
  error status, `reraise`).
- Graceful no-op when required credentials are absent or both enable flags
  are false.
- Logger provider trace-context injection round-trips a live span's
  trace/span id into the log record.
- Selecting `backend: azure_monitor` never reads `databricks_zerobus` config
  and vice versa.

### Platform system tests

Requires a real Azure Monitor OTLP-enabled resource and a real Databricks
workspace in a Zerobus-supported region:

1. emit a span tree and log records from a real Kindling app run against
   each backend; confirm correct `trace_id`/`parent_span_id` linkage after
   each platform's documented visibility latency;
2. force token expiry (short-TTL test credential where obtainable) and
   confirm refresh, not dropped records;
3. confirm graceful no-op outside a supported region/resource configuration
   for either backend;
4. confirm behavior under a burst of spans approaching each backend's
   documented quota — backpressure/retry, not silent data loss.

## Delivery phases and estimate

### Phase 0 — spike and capability lock (3–4 days)

- Reconfirm current preview/beta status and regional availability for both
  destinations against real resources.
- Obtain the authoritative Zerobus table DDL; confirm the Application
  Insights "OTLP support" provisioning flow end to end.
- Prove the gRPC `AuthMetadataPlugin` token-refresh approach against both
  real endpoints; fall back to periodic exporter rebuild only if blocked on
  either.
- Confirm `opentelemetry-api`/`-sdk` version compatibility across Databricks
  Runtime, Fabric, and Synapse's bundled packages.

### Phase 1 — shared provider layer + Azure Monitor backend (4–6 days)

- `OtlpConfig`, `OtlpTraceProvider`, `OtlpLoggerProvider`, the
  `OtlpTokenProvider` interface, `EntraIdTokenProvider`, DI registration,
  config plumbing, unit tests, deprecation notice on
  `kindling_ext_otel_azure`.

### Phase 2 — Databricks Zerobus backend (2–3 days)

- `DatabricksOAuthTokenProvider` behind the same interface, table-setup
  helper/documentation, config plumbing.

### Phase 3 — system tests, migration docs, hardening (3–5 days)

- Full system-test matrix above on both backends, quota/backpressure
  behavior under representative tracing volume, migration guide from
  `kindling_ext_otel_azure`, documentation in
  `docs/contributing/logging_tracing.md` and the new extension's `README.md`.

Expected first production scope: **12–18 engineering days**, split across
two backends behind one interface rather than duplicated per-backend work —
smaller than building the two extensions separately would have been, and
dominated by Phase 0's external unknowns rather than the implementation
itself.

## Alternatives rejected

### Two separate extensions (Azure Monitor Distro-based + a new Zerobus-only one)

Rejected — this was the original framing of this proposal, before checking
Azure Monitor's current OTLP story. It would have meant duplicating the
entire span/log adapter layer across two packages for a difference that
turned out to be "which token provider," not "which telemetry system."

### Keep `kindling_ext_otel_azure` on the Distro path indefinitely, add Zerobus separately

Rejected. The Distro path is Azure Monitor's legacy integration surface;
native OTLP is Azure's own forward direction too. Keeping the Distro as the
long-term Azure path while building the new work on generic OTLP would mean
maintaining two different architectures for what is now the same underlying
protocol.

### Wait for both destinations to reach GA before building anything

Rejected per explicit decision above — moving forward on preview/beta
surfaces now, with Phase 0 as the checkpoint that would surface a
disqualifying change before real implementation effort is spent.

### Add a general metrics abstraction now to support the metrics signal

Rejected for this proposal's scope, for the reasons in "Key design
decisions" above — no core meter contract exists, and both vendors add
material extra complexity on the metrics path specifically.

## Open questions / decision requested

1. **Package/extension name.** `kindling_ext_otel_azure` is taken and its
   scope no longer matches (it would cover Databricks too). Candidates:
   `spark-kindling-ext-otel` (generic, backend chosen by config) or
   `spark-kindling-ext-otlp`. Needs a decision before Phase 1 scaffolding.
2. Approve the `OtlpTokenProvider` interface and in-process gRPC token
   refresh as the default auth strategy, with a Collector documented as a
   supported alternative.
3. Approve that resource/table provisioning is a one-time, out-of-band step
   for both backends (not part of `kindling migrate`), pending Phase 0
   recovering the authoritative Zerobus DDL and confirming the Azure
   provisioning flow.
4. Confirm the deprecation timeline for `kindling_ext_otel_azure` — how many
   releases it stays supported alongside the new extension before removal.
