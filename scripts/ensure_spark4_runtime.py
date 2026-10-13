"""Provision (or reuse) an isolated Spark 4.0 test venv (``.venv-spark4``).

The repo's main environment pins pyspark <4.0 and delta-spark <4.0, but
Databricks DBR 17.3 LTS (the CI cluster, and likely production) runs Spark 4.0
on Scala 2.13 / Python 3.12. This script builds a separate venv for running the
local test suites against Spark 4 without touching the default environment or
the Spark 3.5 pins in uv.lock:

* every non-Spark dependency is installed at the exact version uv.lock pins
  (``uv export --frozen --group dev``), so the lane differs from the default
  environment only in its Spark stack;
* the Spark stack is replaced by ``SPARK4_REQUIREMENTS`` -- pyspark 4.0 with
  the ``connect`` extra (so tests can also run over Spark Connect), the
  matching delta-spark 4.0 release, and pins for the Connect gRPC stack. ``configure_spark_with_delta_pip`` derives
  the Maven coordinates (``io.delta:delta-spark_2.13:<version>``) from the
  installed delta-spark, so the Scala 2.13 jars follow automatically.

The venv is rebuilt whenever uv.lock or the Spark pins change (tracked by a
stamp file), otherwise reused. The venv's ``bin/`` directory is printed on
stdout; ``poe test-spark4`` puts it first on PATH.

Usage:
    poe spark4-runtime            # provision/reuse .venv-spark4
    python scripts/ensure_spark4_runtime.py [venv_dir]

Requires ``uv`` on PATH and Java 17+ (Spark 4.x floor) to run the tests.
"""

import hashlib
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_VENV_DIR = Path(".venv-spark4")
# DBR 17.3 LTS ships Python 3.12; uv downloads a managed interpreter if needed.
PYTHON_VERSION = "3.12"
# Spark 4.0 line (DBR 17.x). delta-spark 4.0.x is built for Spark 4.0 / Scala 2.13.
# The Spark Connect client's gRPC stack is not in uv.lock (nothing in the default
# environment needs it), so it is pinned here to keep the venv reproducible.
SPARK4_REQUIREMENTS = [
    "pyspark[connect]==4.0.1",
    "delta-spark==4.0.1",
    "grpcio==1.84.0",
    "grpcio-status==1.80.0",
    "googleapis-common-protos==1.75.0",
]
# Lock entries replaced by SPARK4_REQUIREMENTS (py4j is pinned exactly by pyspark).
REPLACED_PACKAGES = {"pyspark", "delta-spark", "py4j"}
STAMP_FILE = ".kindling-spark4-stamp"

_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def filter_locked_requirements(exported: str) -> str:
    """Drop the Spark 3.5 stack from ``uv export`` output, keep everything else."""
    kept = []
    for line in exported.splitlines():
        match = _NAME_RE.match(line)
        if (
            match
            and not line.lstrip().startswith("-")
            and (_normalize(match.group(1)) in REPLACED_PACKAGES)
        ):
            continue
        kept.append(line)
    return "\n".join(kept) + "\n"


def _stamp(repo_root: Path) -> str:
    digest = hashlib.sha256()
    digest.update((repo_root / "uv.lock").read_bytes())
    digest.update("\n".join([PYTHON_VERSION, *SPARK4_REQUIREMENTS]).encode())
    return digest.hexdigest()


def _uv() -> str:
    uv = shutil.which("uv")
    if not uv:
        sys.exit("uv is required to provision the Spark 4 runtime (pip install uv)")
    return uv


def ensure_spark4_runtime(venv_dir: Path = DEFAULT_VENV_DIR) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    venv_dir = Path(venv_dir)
    if not venv_dir.is_absolute():
        venv_dir = repo_root / venv_dir
    python = venv_dir / "bin" / "python"
    stamp_path = venv_dir / STAMP_FILE
    expected = _stamp(repo_root)

    if python.exists() and stamp_path.exists() and stamp_path.read_text().strip() == expected:
        print(venv_dir / "bin")
        return venv_dir / "bin"

    uv = _uv()
    print(f"Creating Spark 4 runtime venv at {venv_dir} ...", file=sys.stderr)
    subprocess.run(
        [uv, "venv", "--clear", "--python", PYTHON_VERSION, str(venv_dir)],
        check=True,
        cwd=repo_root,
    )
    exported = subprocess.run(
        [uv, "export", "--frozen", "--group", "dev", "--no-hashes", "--no-header"],
        check=True,
        cwd=repo_root,
        capture_output=True,
        text=True,
    ).stdout
    with tempfile.TemporaryDirectory() as tmp:
        requirements = Path(tmp) / "requirements-spark4.txt"
        requirements.write_text(filter_locked_requirements(exported))
        subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(python),
                "-r",
                str(requirements),
                *SPARK4_REQUIREMENTS,
            ],
            check=True,
            cwd=repo_root,
        )
    stamp_path.write_text(expected + "\n")
    print(venv_dir / "bin")
    return venv_dir / "bin"


def main() -> None:
    ensure_spark4_runtime(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_VENV_DIR)


if __name__ == "__main__":
    main()
