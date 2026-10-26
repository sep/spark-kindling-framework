"""Runtime manifest: what one Kindling initialization ran on.

Built once per ``initialize_framework`` (see docs/proposals/runtime_manifest.md):
begun before the dependency install, so every change an install makes is
recorded, completed once platform services exist, and extended after an app's
dependency install. It holds the identity of the run (a per-initialization
``run_id`` and the executor run ids made under it), the platform, engine,
Kindling and app versions, the key packages, every package change an install
made, and two fingerprints:

- ``runtime_fingerprint``: platform, runtime version, Python major.minor and
  the key packages; it changes when the vendor updates the runtime;
- ``package_fingerprint``: the whole installed set; it changes when any
  install replaces anything.

Bootstrap logs it on one INFO line and emits a scalar-only
``runtime_manifest`` span right after initialization, before an app runs.
Provenance stamps carry its ``run_id``. Every field is best effort: a value
that can't be read is None, never an error. Nothing here runs a Spark
statement; Spark details come from ``spark.version`` and ``spark.conf``,
each guarded.
"""

import hashlib
import json
import sys
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

_LOCK = threading.Lock()
_CURRENT: Optional["RuntimeManifest"] = None
# The key distributions the manifest was completed with, so a later install
# refreshes key_packages (including one it newly adds) the same way.
_KEY_DISTRIBUTIONS: frozenset = frozenset()

_EXTENSION_PREFIX = "spark-kindling-ext-"


@dataclass(frozen=True)
class PackageChange:
    """One distribution an install added or replaced."""

    name: str
    before: Optional[str]
    after: Optional[str]
    cause: str


@dataclass
class RuntimeManifest:
    """What one Kindling initialization ran on (see module docstring)."""

    run_id: str
    started_at: str
    execution_ids: List[str] = field(default_factory=list)
    spark_application_id: Optional[str] = None
    platform: Optional[str] = None
    platform_runtime_version: Optional[str] = None
    spark_connect: Optional[bool] = None
    python_version: str = field(
        default_factory=lambda: ".".join(str(part) for part in sys.version_info[:3])
    )
    spark_version: Optional[str] = None
    pyspark_version: Optional[str] = None
    delta_version: Optional[str] = None
    kindling_version_requested: Optional[str] = None
    kindling_version: Optional[str] = None
    extensions: Dict[str, str] = field(default_factory=dict)
    app_name: Optional[str] = None
    app_version: Optional[str] = None
    key_packages: Dict[str, str] = field(default_factory=dict)
    package_changes: List[PackageChange] = field(default_factory=list)
    runtime_fingerprint: Optional[str] = None
    package_fingerprint: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """The manifest as JSON-serializable data."""
        return asdict(self)

    def summary(self) -> str:
        """The one-line INFO form: main versions and fingerprints."""
        extensions = ",".join(
            f"{name[len(_EXTENSION_PREFIX):]}={version}"
            for name, version in sorted(self.extensions.items())
        )
        requested = (
            f" (requested {self.kindling_version_requested})"
            if self.kindling_version_requested
            and self.kindling_version_requested != self.kindling_version
            else ""
        )
        parts = [
            f"run_id={self.run_id}",
            f"platform={self.platform or '?'}",
            f"runtime={self.platform_runtime_version or '?'}",
            f"spark={self.spark_version or '?'}",
            f"pyspark={self.pyspark_version or '?'}",
            f"delta={self.delta_version or '?'}",
            f"python={self.python_version}",
            f"kindling={self.kindling_version or '?'}{requested}",
            f"extensions=[{extensions}]",
            f"app={self.app_name or '-'}{('@' + self.app_version) if self.app_version else ''}",
            f"package_changes={len(self.package_changes)}",
            f"runtime_fingerprint={(self.runtime_fingerprint or '?')[:12]}",
            f"package_fingerprint={(self.package_fingerprint or '?')[:12]}",
        ]
        return "Runtime manifest: " + " ".join(parts)

    def span_details(self) -> Dict[str, str]:
        """Scalar attributes for the ``runtime_manifest`` span (span
        attribute size limits: no package lists)."""
        details = {
            "run_id": self.run_id,
            "platform": self.platform,
            "platform_runtime_version": self.platform_runtime_version,
            "spark_connect": None if self.spark_connect is None else str(self.spark_connect),
            "spark_application_id": self.spark_application_id,
            "spark_version": self.spark_version,
            "pyspark_version": self.pyspark_version,
            "delta_version": self.delta_version,
            "python_version": self.python_version,
            "kindling_version": self.kindling_version,
            "kindling_version_requested": self.kindling_version_requested,
            "extension_count": str(len(self.extensions)),
            "app_name": self.app_name,
            "app_version": self.app_version,
            "package_change_count": str(len(self.package_changes)),
            "runtime_fingerprint": self.runtime_fingerprint,
            "package_fingerprint": self.package_fingerprint,
        }
        return {key: value for key, value in details.items() if value is not None}


# ── Lifecycle ────────────────────────────────────────────────────────────────


def begin_runtime_manifest(
    *,
    platform: Optional[str] = None,
    kindling_version_requested: Optional[str] = None,
    app_name: Optional[str] = None,
) -> RuntimeManifest:
    """Start this initialization's manifest with a new ``run_id``."""
    global _CURRENT
    manifest = RuntimeManifest(
        run_id=str(uuid.uuid4()),
        started_at=datetime.now(timezone.utc).isoformat(),
        platform=_text(platform),
        kindling_version_requested=_text(kindling_version_requested),
        app_name=_text(app_name),
    )
    with _LOCK:
        _CURRENT = manifest
    return manifest


def get_runtime_manifest() -> Optional[RuntimeManifest]:
    """The current initialization's manifest, or None before one began."""
    with _LOCK:
        return _CURRENT


def current_run_id() -> Optional[str]:
    """The current initialization's ``run_id``, or None."""
    manifest = get_runtime_manifest()
    return manifest.run_id if manifest is not None else None


def reset_runtime_manifest() -> None:
    """Forget the current manifest (tests)."""
    global _CURRENT, _KEY_DISTRIBUTIONS
    with _LOCK:
        _CURRENT = None
        _KEY_DISTRIBUTIONS = frozenset()


def complete_runtime_manifest(
    *,
    installed: Mapping[str, str],
    key_distributions: Iterable[str],
    spark: Any = None,
    config_service: Any = None,
    platform: Optional[str] = None,
) -> Optional[RuntimeManifest]:
    """Fill in versions and fingerprints once platform services exist.

    ``installed`` maps normalized distribution names to versions (as
    import resolves them); ``key_distributions`` are the runtime packages
    the bootstrap pins.
    """
    global _KEY_DISTRIBUTIONS
    manifest = get_runtime_manifest()
    if manifest is None:
        return None
    with _LOCK:
        _KEY_DISTRIBUTIONS = frozenset(key_distributions)
        if platform:
            manifest.platform = _text(platform)
        manifest.spark_version = _spark_version(spark, config_service)
        manifest.platform_runtime_version = _platform_runtime_version(config_service)
        manifest.spark_application_id = _spark_conf(spark, "spark.app.id")
        manifest.spark_connect = _is_spark_connect(spark)
        _apply_installed(manifest, installed)
    return manifest


def _apply_installed(manifest: "RuntimeManifest", installed: Mapping[str, str]) -> None:
    """Set every package-derived field from ``installed`` and recompute both
    fingerprints (caller holds the lock)."""
    manifest.pyspark_version = installed.get("pyspark")
    manifest.delta_version = installed.get("delta-spark")
    manifest.kindling_version = installed.get("spark-kindling")
    manifest.extensions = {
        name: version
        for name, version in sorted(installed.items())
        if name.startswith(_EXTENSION_PREFIX)
    }
    manifest.key_packages = {
        name: installed[name] for name in sorted(_KEY_DISTRIBUTIONS) if name in installed
    }
    manifest.runtime_fingerprint = _runtime_fingerprint(manifest)
    manifest.package_fingerprint = _package_fingerprint(installed)


def record_package_changes(
    before: Mapping[str, str], after: Mapping[str, str], cause: str
) -> List[PackageChange]:
    """Record every distribution an install added or replaced.

    ``before`` and ``after`` map distribution names to versions. Returns the
    changes (also when no manifest has begun). Once the bootstrap completed
    the manifest, every package-derived field (versions, extensions, key
    packages) and both fingerprints are refreshed from ``after``.
    """
    changes = [
        PackageChange(name=name, before=before.get(name), after=version, cause=cause)
        for name, version in sorted(after.items())
        if before.get(name) != version
    ]
    manifest = get_runtime_manifest()
    if manifest is not None and changes:
        with _LOCK:
            manifest.package_changes.extend(changes)
            if manifest.package_fingerprint is not None:
                _apply_installed(manifest, after)
    return changes


def record_app(app_name: Optional[str], app_version: Optional[str] = None) -> None:
    """Record the app this initialization runs."""
    manifest = get_runtime_manifest()
    if manifest is None:
        return
    with _LOCK:
        manifest.app_name = _text(app_name) or manifest.app_name
        manifest.app_version = _text(app_version) or manifest.app_version


def record_execution_id(execution_id: Optional[str]) -> None:
    """Record an executor run id made under this initialization."""
    manifest = get_runtime_manifest()
    if manifest is None or not execution_id:
        return
    with _LOCK:
        if execution_id not in manifest.execution_ids:
            manifest.execution_ids.append(str(execution_id))


class RuntimeManifestExecutionRecorder:
    """Records the executor run ids made under this initialization, from
    the signals that announce a run: ``orchestrator.execution_started``
    (generation executor), ``datapipes.before_run`` (sequential executor)
    and ``streaming.orchestrator_started``. A DAG-mode ``before_run``
    (``delegated_to_orchestrator``) is skipped: that run's id is the one
    ``orchestrator.execution_started`` carries."""

    _SIGNALS = (
        "orchestrator.execution_started",
        "datapipes.before_run",
        "streaming.orchestrator_started",
    )

    def __init__(self, signal_provider):
        self._signals = signal_provider
        self._registered = False

    def register(self) -> None:
        """Connect the handler. Idempotent."""
        if self._registered or self._signals is None:
            return
        for name in self._SIGNALS:
            signal = self._signals.get_signal(name) or self._signals.create_signal(name)
            signal.connect(self._on_run_started, weak=False)
        self._registered = True

    def _on_run_started(self, sender, *, run_id=None, delegated_to_orchestrator=False, **kwargs):
        if not delegated_to_orchestrator:
            record_execution_id(run_id)
        return None


# ── Probes ───────────────────────────────────────────────────────────────────


def _text(value: Any) -> Optional[str]:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _config_text(config_service: Any, key: str) -> Optional[str]:
    if config_service is None:
        return None
    try:
        return _text(config_service.get(key))
    except Exception:
        return None


def _spark_version(spark: Any, config_service: Any) -> Optional[str]:
    discovered = _config_text(config_service, "kindling.runtime.features.spark.version")
    if discovered:
        return discovered
    if spark is None:
        return None
    try:
        return _text(spark.version)
    except Exception:
        return None


def _platform_runtime_version(config_service: Any) -> Optional[str]:
    # Feature discovery reads the Databricks runtime version; Fabric and
    # Synapse runtime versions are not read yet (unverified, see the proposal).
    return _config_text(config_service, "kindling.runtime.features.databricks.runtime_version")


def _spark_conf(spark: Any, key: str) -> Optional[str]:
    if spark is None:
        return None
    try:
        return _text(spark.conf.get(key, None))
    except Exception:
        return None


def _is_spark_connect(spark: Any) -> Optional[bool]:
    if spark is None:
        return None
    return type(spark).__module__.startswith("pyspark.sql.connect")


def _digest(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _runtime_fingerprint(manifest: RuntimeManifest) -> str:
    python_major_minor = ".".join(manifest.python_version.split(".")[:2])
    return _digest(
        {
            "platform": manifest.platform,
            "runtime": manifest.platform_runtime_version,
            "python": python_major_minor,
            "packages": manifest.key_packages,
        }
    )


def _package_fingerprint(installed: Mapping[str, str]) -> str:
    return _digest(sorted(installed.items()))
