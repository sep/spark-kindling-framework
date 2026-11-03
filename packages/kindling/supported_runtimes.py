"""The platform runtimes Kindling is system-tested on, and matching the
running runtime against them.

Each entry lists the key packages (the bootstrap's
``_RUNTIME_KEY_DISTRIBUTIONS``) that runtime ships, from its release notes.
CI uses the same list to check that every extension installs there without
replacing them (``scripts/check_extension_runtime_packages.py``), and the
bootstrap records in the runtime manifest whether this runtime is one of
them (``match_runtime``), warning when it isn't. Update an entry when a
supported runtime changes, and add one when a runtime joins the system
tests.

This module imports nothing from Kindling or Spark: the CI check loads it by
file path.
"""

import re
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, FrozenSet, Mapping, Optional, Tuple


@dataclass(frozen=True)
class SupportedRuntime:
    name: str
    platform: str
    # The platform's own version label where the platform reports one (the
    # Databricks Runtime "17.3"); None where it doesn't (Fabric, Synapse).
    runtime_version: Optional[str]
    python: str
    packages: Mapping[str, str]
    # Shipped packages an install may upgrade on this runtime: nothing the
    # runtime itself runs needs the shipped version. The bootstrap doesn't
    # pin them there, and the CI extension check doesn't either.
    replaceable: FrozenSet[str] = frozenset()


SUPPORTED_RUNTIMES: Tuple[SupportedRuntime, ...] = (
    # Databricks Runtime 17.3 LTS (Python 3.12.3)
    SupportedRuntime(
        name="dbr17.3",
        platform="databricks",
        runtime_version="17.3",
        python="3.12",
        packages={
            "azure-core": "1.34.0",
            "azure-identity": "1.20.0",
            "msal": "1.32.3",
            "numpy": "2.1.3",
            "opentelemetry-api": "1.32.1",
            "opentelemetry-sdk": "1.32.1",
            "opentelemetry-semantic-conventions": "0.53b1",
            "pandas": "2.2.3",
            "protobuf": "5.29.4",
            # Spark is bundled with the runtime, not a pip install; these
            # are the versions it provides (Apache Spark 4.0.0).
            "py4j": "0.10.9.9",
            "pyarrow": "19.0.1",
            "pyspark": "4.0.0",
            "typing-extensions": "4.12.2",
        },
    ),
    # Fabric Runtime 2.0 (Spark 4.1, Python 3.13). Fabric ships no
    # azure-monitor-opentelemetry, and OpenTelemetry 1.16, which is older than
    # the OpenTelemetry Azure extension supports (>=1.20). Only mlflow-skinny
    # depends on it there (opentelemetry-api/-sdk >=1.9.0,<3), so it may be
    # upgraded.
    SupportedRuntime(
        name="fabric2.0",
        platform="fabric",
        runtime_version=None,
        python="3.13",
        packages={
            "azure-core": "1.39.0",
            "azure-identity": "1.25.3",
            "msal": "1.37.0",
            "numpy": "2.4.1",
            "opentelemetry-api": "1.16.0",
            "opentelemetry-sdk": "1.16.0",
            "opentelemetry-semantic-conventions": "0.37b0",
            "pandas": "2.3.3",
            "protobuf": "5.29.6",
            "py4j": "0.10.9.9",
            "pyarrow": "24.0.0",
            # The runtime ships a vendor build (4.1.1.5.5.20260428.5, not on PyPI);
            # its public base release stands in for it.
            "pyspark": "4.1.1",
            "typing-extensions": "4.16.0",
        },
        replaceable=frozenset(
            {"opentelemetry-api", "opentelemetry-sdk", "opentelemetry-semantic-conventions"}
        ),
    ),
    # Azure Synapse Runtime for Apache Spark 3.5 (Python 3.11). azure-identity
    # and msal ship as conda builds versioned 2025.03.01; the versions they
    # report at runtime stand in for them.
    SupportedRuntime(
        name="synapse3.5",
        platform="synapse",
        runtime_version=None,
        python="3.11",
        packages={
            "azure-core": "1.35.0",
            "azure-identity": "1.20.0",
            "azure-monitor-opentelemetry": "1.7.0",
            "msal": "1.31.1",
            "numpy": "1.26.4",
            "opentelemetry-api": "1.36.0",
            "opentelemetry-sdk": "1.36.0",
            "opentelemetry-semantic-conventions": "0.57b0",
            "pandas": "2.2.3",
            "protobuf": "4.25.3",
            "py4j": "0.10.9.7",
            "pyarrow": "19.0.0",
            # The runtime ships a vendor build (3.5.1.5.4.20240407, not on PyPI);
            # its public base release stands in for it.
            "pyspark": "3.5.1",
            "typing-extensions": "4.12.2",
        },
    ),
)


def replaceable_packages(platform: Optional[str]) -> FrozenSet[str]:
    """The shipped packages an install may upgrade on any tested runtime of
    ``platform``."""
    name = str(platform or "").strip().lower()
    return frozenset(
        package
        for runtime in SUPPORTED_RUNTIMES
        if runtime.platform == name
        for package in runtime.replaceable
    )


# Local development: recorded, never called untested.
_UNCHECKED_PLATFORMS = frozenset({"standalone"})


@dataclass(frozen=True)
class RuntimeDifference:
    """A key package (or Python) whose version differs from the tested one."""

    name: str
    tested: str
    installed: str
    # True when major.minor differs; patch-level differences are recorded
    # but never warned about.
    major_minor: bool


@dataclass(frozen=True)
class RuntimeMatch:
    """How the running runtime compares with the system-tested ones.

    ``status`` is ``tested`` (a tested runtime; any differences are
    patch-level), ``partial`` (closest to ``runtime`` but a key package or
    Python differs at major.minor), ``untested`` (no tested runtime for this
    platform or platform version) or ``unchecked`` (standalone, or the
    platform is unknown).
    """

    status: str
    runtime: Optional[str] = None
    differences: Tuple[RuntimeDifference, ...] = field(default_factory=tuple)

    @property
    def should_warn(self) -> bool:
        return self.status in ("partial", "untested")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def label(self) -> str:
        """``tested:dbr17.3``, ``partial:synapse3.5``, ``untested`` or ``unchecked``."""
        return f"{self.status}:{self.runtime}" if self.runtime else self.status

    def describe(self, platform: Optional[str], runtime_version: Optional[str]) -> str:
        """The warning text for ``partial`` and ``untested``."""
        if self.status == "partial":
            differing = ", ".join(
                f"{d.name} {d.installed} (tested {d.tested})"
                for d in self.differences
                if d.major_minor
            )
            return (
                f"Runtime differs from the system-tested {self.runtime}: {differing}. "
                "Kindling may behave differently here."
            )
        version = f" {runtime_version}" if runtime_version else ""
        tested = ", ".join(r.name for r in SUPPORTED_RUNTIMES)
        return (
            f"Untested runtime: {platform}{version} is not one Kindling is "
            f"system-tested on ({tested})."
        )


def match_runtime(
    platform: Optional[str],
    runtime_version: Optional[str],
    installed: Mapping[str, str],
    python_version: Optional[str] = None,
) -> RuntimeMatch:
    """Compare the running runtime with ``SUPPORTED_RUNTIMES``.

    The candidates are the tested runtimes for ``platform``; on a platform
    that reports its version (Databricks), only those of that version (a
    version that isn't listed, or doesn't parse, is untested). The
    closest candidate (fewest major.minor differences, then fewest
    differences) is the match. Only the packages a tested runtime ships are
    compared, and a package that isn't installed is skipped. Versions are
    normalized: a vendor build compares by its base release
    (``3.5.1.5.4.20240407`` is ``3.5.1``), and a conda build versioned by date
    (``2023.12.01``) is skipped. A difference in one of a runtime's
    ``replaceable`` packages is recorded but never counts as major.minor: an
    install is allowed to upgrade it.
    """
    platform = (platform or "").strip().lower()
    if not platform or platform in _UNCHECKED_PLATFORMS:
        return RuntimeMatch(status="unchecked")
    candidates = [r for r in SUPPORTED_RUNTIMES if r.platform == platform]
    if (runtime_version or "").strip() and any(r.runtime_version for r in candidates):
        # A reported version must be a listed one; an unparseable label
        # (Databricks serverless "client.1.13") is untested, not a package match.
        label = _version_label(runtime_version)
        candidates = [r for r in candidates if label is not None and r.runtime_version == label]
    if not candidates:
        return RuntimeMatch(status="untested")

    python = python_version or ".".join(str(part) for part in sys.version_info[:3])
    best = min(
        ((r, _differences(r, installed, python)) for r in candidates),
        key=lambda pair: (sum(d.major_minor for d in pair[1]), len(pair[1])),
    )
    runtime, differences = best
    status = "partial" if any(d.major_minor for d in differences) else "tested"
    return RuntimeMatch(status=status, runtime=runtime.name, differences=tuple(differences))


def _differences(runtime: SupportedRuntime, installed: Mapping[str, str], python: str):
    differences = []
    python_major_minor = ".".join(python.split(".")[:2])
    if python_major_minor != runtime.python:
        differences.append(
            RuntimeDifference("python", runtime.python, python_major_minor, major_minor=True)
        )
    for name, tested in sorted(runtime.packages.items()):
        version = installed.get(name)
        if not version or _same_version(version, tested):
            continue
        if _is_date_version(version) and not _is_date_version(tested):
            continue
        differences.append(
            RuntimeDifference(
                name,
                tested,
                version,
                major_minor=name not in runtime.replaceable
                and _release(version)[:2] != _release(tested)[:2],
            )
        )
    return differences


def _same_version(installed: str, tested: str) -> bool:
    installed, tested = installed.strip().lower(), tested.strip().lower()
    # A vendor build appends to the public release it is based on.
    return installed == tested or installed.startswith(tested + ".")


def _release(version: str) -> Tuple[int, ...]:
    match = re.match(r"\d+(?:\.\d+)*", version.strip())
    return tuple(int(part) for part in match.group(0).split(".")) if match else ()


def _is_date_version(version: str) -> bool:
    release = _release(version)
    return bool(release) and release[0] >= 1900


def _version_label(runtime_version: Optional[str]) -> Optional[str]:
    """``17.3`` from a Databricks ``17.3.x-scala2.13``."""
    match = re.match(r"\s*(\d+\.\d+)", runtime_version or "")
    return match.group(1) if match else None
