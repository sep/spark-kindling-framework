"""Provision (or reuse) an isolated pyspark 4.1 venv for spark-pipelines.

The repo's main environment pins pyspark <4.0 (delta-spark compatibility),
but ``spark-pipelines dry-run`` needs Spark 4.1+. The two coexist because
the dry-run harness shells out to the CLI: this script creates a dedicated
venv (default ``.venv-sdp41/``, gitignored) containing only
``pyspark[pipelines]``, and prints the CLI path. The harness derives
SPARK_HOME/PYSPARK_PYTHON from that path automatically.

Usage:
    poe sdp-runtime            # provision/reuse .venv-sdp41
    python scripts/ensure_sdp_runtime.py [venv_dir]

Requires Java 17+ (Spark 4.x floor) to actually run dry-runs.
"""

import subprocess
import sys
import venv
from pathlib import Path

DEFAULT_VENV_DIR = Path(".venv-sdp41")
PYSPARK_REQUIREMENT = "pyspark[pipelines]>=4.1"


def ensure_sdp_runtime(venv_dir: Path = DEFAULT_VENV_DIR) -> Path:
    venv_dir = Path(venv_dir)
    cli = venv_dir / "bin" / "spark-pipelines"
    if not cli.exists():
        print(f"Creating SDP runtime venv at {venv_dir} ...", file=sys.stderr)
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        subprocess.run(
            [str(venv_dir / "bin" / "pip"), "install", "--quiet", PYSPARK_REQUIREMENT],
            check=True,
        )
    print(cli)
    return cli


def main() -> None:
    ensure_sdp_runtime(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_VENV_DIR)


if __name__ == "__main__":
    main()
