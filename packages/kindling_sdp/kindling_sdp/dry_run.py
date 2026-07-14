"""Local pipeline validation harness: ephemeral spec + ``spark-pipelines dry-run``.

The proposal's bridge section makes ``kindling_sdp`` the deliberate owner
of one piece of deployment glue: a minimal local validation harness that
writes a pipeline spec and invokes ``spark-pipelines dry-run`` against the
declared graph — a unit-test-tier gate for the pipeline with no Databricks
(and no cluster) in the loop.

Spec format and CLI per the Spark 4.1 Declarative Pipelines programming
guide: the spec file (``spark-pipeline.yml``) requires ``name``,
``libraries`` (glob includes of transformation sources), and ``storage``
(streaming-checkpoint directory); ``catalog``/``database`` are optional;
``spark-pipelines dry-run --spec <path>`` validates without reading or
writing data.

The definitions file the spec points at is app-authored (the proposal's
fixed bootstrap surface — initialize, register, ``declare_pipeline()``);
this harness never generates application code.
"""

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


class SparkPipelinesCliNotFoundError(RuntimeError):
    """The ``spark-pipelines`` executable is not on PATH."""


@dataclass(frozen=True)
class DryRunResult:
    """Outcome of one ``spark-pipelines dry-run`` invocation."""

    ok: bool
    returncode: int
    stdout: str
    stderr: str
    spec_path: Path


def write_pipeline_spec(
    spec_dir: Path,
    name: str,
    definitions_globs: List[str],
    storage: Optional[str] = None,
    catalog: Optional[str] = None,
    database: Optional[str] = None,
) -> Path:
    """Write ``spark-pipeline.yml`` into ``spec_dir`` and return its path.

    ``definitions_globs`` are include patterns relative to the spec file
    (e.g. ``["definitions/*.py"]``). ``storage`` defaults to a ``storage/``
    directory next to the spec — required by the spec schema even for a
    dry-run.
    """
    spec_dir = Path(spec_dir)
    spec_dir.mkdir(parents=True, exist_ok=True)
    storage_value = storage or (spec_dir / "storage").resolve().as_uri()

    lines = [f"name: {name}", "libraries:"]
    for pattern in definitions_globs:
        lines.append("  - glob:")
        lines.append(f"      include: {pattern}")
    lines.append(f"storage: {storage_value}")
    if catalog:
        lines.append(f"catalog: {catalog}")
    if database:
        lines.append(f"database: {database}")

    spec_path = spec_dir / "spark-pipeline.yml"
    spec_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return spec_path


def dry_run(
    spec_path: Path,
    executable: str = "spark-pipelines",
    timeout_seconds: int = 600,
) -> DryRunResult:
    """Run ``spark-pipelines dry-run --spec <spec_path>`` and capture output.

    ``executable`` is overridable for tests and for environments where the
    Spark 4.1 CLI lives outside PATH. Raises
    :class:`SparkPipelinesCliNotFoundError` (with install guidance) when
    the executable cannot be found at all; a failed validation is NOT an
    exception — it is a ``DryRunResult`` with ``ok=False`` carrying the
    CLI's diagnostics.
    """
    spec_path = Path(spec_path)
    resolved = shutil.which(executable)
    if resolved is None:
        raise SparkPipelinesCliNotFoundError(
            f"'{executable}' not found on PATH. The spark-pipelines CLI "
            "ships with Spark 4.1+: pip install 'pyspark[pipelines]>=4.1'"
        )

    completed = subprocess.run(
        [resolved, "dry-run", "--spec", str(spec_path)],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    return DryRunResult(
        ok=completed.returncode == 0,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        spec_path=spec_path,
    )
