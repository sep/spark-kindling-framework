# Proposal: ABFSS Local Auth via Azure CLI Token Provider

## Goal

Allow standalone (local dev) Spark execution to access `abfss://` paths using the developer's
existing `az login` session — no service principal, no storage key, no extra RBAC beyond what
the developer already has on the storage account.

## Approach

Ship a Java JAR (`kindling-abfss-local-auth.jar`) that implements Hadoop's
`org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee` interface. The provider
caches tokens in memory, refreshes them proactively before expiry, and acquires them by calling
`az account get-access-token`. The `StandaloneService` detects whether `az` is on PATH and
automatically injects the provider into the Spark session's Hadoop config.

No env vars to manage, no token rotation logic in Python — Hadoop drives the lifecycle via
`getAccessToken()` / `getExpiryTime()`, and the provider handles caching and refresh internally.

## Why Not the Alternatives

| Option | Problem |
|---|---|
| Storage account key via management API | Requires "Storage Account Key Operator" RBAC — more than read access |
| SAS token | Expires; requires generation and rotation logic in Python |
| Service principal config | Requires storing credentials — defeats the point of local dev convenience |
| Managed Identity | Not available on a developer workstation |
| Azure Identity SDK | Adds a Java dependency; more than needed for a dev-only tool |
| Custom JAR (`CustomTokenProviderAdaptee`) calling `az` | Self-contained, no RBAC beyond what the dev already has, auto-refreshes |

## Non-Goals

The provider must not use Managed Identity, Service Principals, client secrets, client
certificates, the Azure Identity SDK, or interactive browser auth. It relies exclusively on the
authenticated Azure CLI session already present on the developer workstation.

This is a **development-only** mechanism. It must never be used on Databricks / Fabric / Synapse
or in any environment where platform-managed auth is available.

## Components

### 1. Java token provider JAR

A single Java class implementing
`org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee`.

#### Token acquisition

Call `az account get-access-token --resource https://storage.azure.com/ -o json` and parse the
JSON response to extract both the access token and the expiry timestamp:

```java
// JSON output fields vary by az CLI version — handle all known variants
String token   = json.getString("accessToken");
String expires = json.optString("expiresOn",   // older CLI versions
                 json.optString("expires_on",   // newer CLI versions
                 null));
```

Parse `expires` as an ISO-8601 or `YYYY-MM-DD HH:mm:ss.SSSSSS` datetime (both appear in the
wild) and convert to a `java.util.Date`. Do not hardcode a 1-hour offset — use the timestamp
the CLI actually returns.

#### Caching and refresh

Cache the token in memory. On each `getAccessToken()` call, return the cached token if it has
more than 5 minutes remaining. If it is within the refresh buffer (default: 5 minutes), fetch a
new token before returning. The refresh buffer should be configurable via Hadoop config:

```
fs.azure.account.oauth.provider.refresh.buffer.seconds   (default: 300)
```

#### Thread safety

Multiple Hadoop filesystem operations may call `getAccessToken()` concurrently. The
implementation must ensure only one thread executes the CLI at a time. Use `synchronized` or an
`AtomicBoolean` refresh-in-progress flag to prevent redundant concurrent refreshes.

#### Fail-fast behavior

Throw `IOException` with a clear, actionable message when:

- `az` is not found on PATH: `"Azure CLI not found. Install az and run 'az login'."`
- CLI exits non-zero: `"az account get-access-token failed (exit <N>): <stderr>. Run 'az login'."`
- JSON is missing expected fields: `"Unexpected az CLI output — could not parse access token."`

#### Logging

Use `org.apache.commons.logging.Log` (already on the Spark classpath). Log at `INFO` for:
- token acquisition (account name, expiry time)
- proactive refresh (previous expiry, new expiry)

Log at `WARN` for CLI failures before rethrowing.

#### Configurable CLI path

Support a Hadoop config key for a non-default `az` location:

```
fs.azure.account.oauth.provider.az.path   (default: "az")
```

#### Skeleton

```java
public class AzureCliTokenProvider implements CustomTokenProviderAdaptee {

    private static final Log LOG = LogFactory.getLog(AzureCliTokenProvider.class);

    static final String CONF_AZ_PATH      = "fs.azure.account.oauth.provider.az.path";
    static final String CONF_REFRESH_SECS = "fs.azure.account.oauth.provider.refresh.buffer.seconds";
    static final int    DEFAULT_REFRESH_SECS = 300;

    private String  azPath;
    private long    refreshBufferMs;
    private String  cachedToken;
    private Date    cachedExpiry;

    @Override
    public synchronized void initialize(Configuration conf, String accountName) {
        azPath          = conf.get(CONF_AZ_PATH, "az");
        refreshBufferMs = conf.getLong(CONF_REFRESH_SECS, DEFAULT_REFRESH_SECS) * 1000L;
    }

    @Override
    public synchronized String getAccessToken() throws IOException {
        if (cachedToken != null && cachedExpiry != null
                && cachedExpiry.getTime() - System.currentTimeMillis() > refreshBufferMs) {
            return cachedToken;
        }
        // fetch fresh token — sets cachedToken and cachedExpiry
        fetchToken();
        return cachedToken;
    }

    @Override
    public synchronized Date getExpiryTime() {
        return cachedExpiry;
    }

    private void fetchToken() throws IOException { /* ... */ }
}
```

**Build target:** Compile with `--release 8` (`maven.compiler.release=8`). The `hadoop-azure`
classes are Java 8 bytecode (class file major version 52), so the provider must match to load
cleanly in the same classloader. The devcontainer's Java 21 supports `--release 8` natively —
no separate JDK needed. Using `--release` (not `-source`/`-target`) also restricts the API
surface to Java 8, preventing accidental use of newer APIs that would fail at runtime.

No dependencies beyond `hadoop-common` and `hadoop-azure` (both already on the Spark classpath,
declared `provided` in the pom). Output a JAR with no shading needed.

Bundle the built JAR at `packages/kindling/jars/kindling-abfss-local-auth.jar`.

### 2. `StandaloneService` integration

On platform initialization, before the Spark session is used:

1. Check `shutil.which("az")` — skip silently if not found (local dev without Azure is fine).
2. If present and not opted out, inject account-agnostic Hadoop config:

```python
spark.conf.set("spark.hadoop.fs.azure.account.auth.type", "Custom")
spark.conf.set(
    "spark.hadoop.fs.azure.account.oauth.provider.type",
    "io.kindling.abfss.AzureCliTokenProvider",
)
```

Per-account config (`fs.azure.account.auth.type.<account>.dfs.core.windows.net`) is also
supported by Hadoop and can be used if a developer needs mixed auth across accounts — the
account-agnostic keys are the default for zero-config standalone use.

3. Add the JAR via the same mechanism used for existing Hadoop JARs in standalone mode.

File: `packages/kindling/platform_standalone.py`

### 3. Opt-out config key

```yaml
# settings.yaml
kindling:
  standalone:
    abfss_az_cli_auth: false   # default: true
```

## Spark Configuration Reference

```properties
# Account-agnostic (applies to all storage accounts in the session)
spark.hadoop.fs.azure.account.auth.type=Custom
spark.hadoop.fs.azure.account.oauth.provider.type=io.kindling.abfss.AzureCliTokenProvider

# Optional tuning
spark.hadoop.fs.azure.account.oauth.provider.refresh.buffer.seconds=300
spark.hadoop.fs.azure.account.oauth.provider.az.path=/usr/bin/az
```

## Implementation Checklist

### 1) Java JAR

- [ ] Create `packages/kindling-abfss-local-auth/` with Maven or Gradle build file.
- [ ] Implement `AzureCliTokenProvider implements CustomTokenProviderAdaptee`.
- [ ] Parse `az` JSON output; handle `expiresOn` and `expires_on` field variants.
- [ ] Implement in-memory token cache with configurable 5-minute refresh buffer.
- [ ] Make `getAccessToken()` and `getExpiryTime()` thread-safe.
- [ ] Prevent concurrent refresh storms (single fetch wins; others wait and reuse result).
- [ ] Fail fast with actionable messages for missing CLI, non-zero exit, and parse errors.
- [ ] Log token acquisition and refresh at INFO; CLI failures at WARN.
- [ ] Support configurable `az` path via Hadoop config key.
- [ ] Build and commit compiled JAR to `packages/kindling/jars/`.
- [ ] Add JAR build step to dev setup / CI workflow.

### 2) JAR unit tests

- [ ] Token parsing: valid JSON with `expiresOn` field.
- [ ] Token parsing: valid JSON with `expires_on` field.
- [ ] Token parsing: missing token field throws with clear message.
- [ ] Cache hit: second call within refresh buffer does not invoke CLI again.
- [ ] Cache miss: call within refresh buffer triggers re-fetch.
- [ ] Thread safety: concurrent calls result in exactly one CLI invocation.
- [ ] Configurable refresh buffer is respected.
- [ ] Configurable `az` path is used in process construction.

### 3) `StandaloneService` injection

- [ ] Add `_configure_abfss_local_auth(spark)` helper in `platform_standalone.py`.
- [ ] Call from platform init after the Spark session exists.
- [ ] Guard with `shutil.which("az")` and `abfss_az_cli_auth` config flag.
- [ ] Log clearly when injection is active vs. skipped.

Files: `packages/kindling/platform_standalone.py`

### 4) Python-side unit tests

- [ ] `test_abfss_auth_injected_when_az_available` — mock `shutil.which`, assert Spark configs set.
- [ ] `test_abfss_auth_skipped_when_az_missing` — mock `shutil.which` → `None`, assert no configs.
- [ ] `test_abfss_auth_skipped_when_opted_out` — `abfss_az_cli_auth: false`, assert no configs.

Files: `tests/unit/test_platform_standalone.py`

### 5) Manual integration smoke test

- [ ] `az login`, point an entity at an `abfss://` path, run standalone, confirm reads succeed.
- [ ] Force token expiry (mock or wait), confirm transparent re-fetch.
- [ ] Confirm cloud platform runs (`--platform databricks`) are unaffected.

### 6) Docs

- [ ] Add "Local ABFSS access" section to `docs/guide/local_python_first.md`:
  - Prerequisite: `az login`
  - Zero additional config needed by default
  - Opt-out instructions
  - Security note: dev-only, never for production

## Security Implications

The provider calls the Azure CLI on behalf of the Spark session, using whatever subscription
and permissions are active in the developer's `az login` session. This means:

- **Scope is the developer's identity**, not a least-privilege service principal. A developer
  with broad Azure access can inadvertently read or write data they shouldn't during local runs.
- **Tokens are cached in JVM heap** for up to 1 hour. On a shared machine this could be a
  concern; on a personal developer workstation it is acceptable.
- **This must never be used in production** or on shared infrastructure. The
  `StandaloneService` injection is platform-guarded to prevent accidental use on Databricks /
  Fabric / Synapse.

## Constraints and Caveats

- **Standalone-only.** Injection is guarded by platform type; never runs on cloud platforms.
- **`az` must be on PATH** (or configured via `az.path`). If absent, ABFSS paths fail at
  access time with a Hadoop auth error. The startup log message should make the prerequisite
  clear before any filesystem call is attempted.
- **Account-agnostic by default.** The same provider applies to all storage accounts. For
  mixed-auth scenarios, opt out and configure per-account keys manually.
- **JAR committed to the repo.** Avoids a Java toolchain requirement for Python devs.
  The JAR must be rebuilt and recommitted when the source changes.

## Acceptance Criteria

1. A developer can `az login` and immediately read `abfss://` entities in standalone mode
   with no additional config.
2. Tokens are cached; `az` is not invoked on every filesystem operation.
3. Tokens are refreshed proactively within 5 minutes of expiry without developer intervention.
4. Concurrent filesystem operations do not produce multiple simultaneous CLI invocations.
5. Missing `az`, not-logged-in, and parse failures produce clear, actionable error messages.
6. Skipping (no `az`, or opted out) produces a log line, not a crash.
7. Cloud platform runs are unaffected.
