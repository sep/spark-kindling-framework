# Build System

How this repo builds its wheels and source distributions. Packaging is standard PEP 621 with the
`uv_build` backend; `poe build` is the entry point and the only supported way
to produce a release-shaped `dist/`.

## Build Commands

```bash
poe build                          # every sdist and wheel, into dist/

poe deploy                         # all wheels in dist/ -> Azure storage
poe deploy --platform fabric       # only the runtime wheel (see below)
poe deploy --release latest        # a published release instead of dist/
poe deploy-extension spark-kindling-ext-otel-azure   # one extension from dist/
```

`--platform` no longer selects a platform-specific build: there is one runtime
wheel for every platform. It limits the deploy to that runtime wheel, which
keeps test deploys small and avoids replacing the design-time and extension
wheels in storage.

## What `poe build` Produces

`poe build` runs `scripts/build.py`, which clears `dist/` and runs
`uv build` once per package. Each build writes a source distribution
(`<name>-<v>.tar.gz`) and a wheel built from that sdist, so the wheel attached
to the GitHub release is the same file PyPI receives:

| Wheel | Source | Notes |
|---|---|---|
| `spark_kindling-<v>` | root `pyproject.toml`, module `packages/kindling` | The runtime. Contains every `platform_*.py`; platform dependencies are extras. |
| `spark_kindling_cli-<v>` | `packages/kindling_cli` | Design-time CLI, including the scaffolding templates. |
| `spark_kindling_sdk-<v>` | `packages/kindling_sdk` | Design-time platform SDK. |
| `spark_kindling_ext_<name>-<v>` | `packages/extensions/kindling_ext_<name>` | Each extension, at its own version. |

Runtime, CLI and SDK share one version (bumped together by `poe version`);
extensions version independently.

Where they are published: every wheel is attached to the GitHub release. The
`publish-pypi` job also uploads the wheel and sdist of `spark-kindling`,
`spark-kindling-cli`, `spark-kindling-sdk` and the `databricks`, `sdp`,
`cosmos`, `temporal` and `otel-azure` extensions to PyPI; the `adx`,
`databricks-autoloader` and `visualization` extensions stay GitHub-release-only.
See [release_process.md](./release_process.md#-publishing-to-pypi).

The runtime wheel is installed with the extra for its environment:

| Extra | Adds |
|---|---|
| `synapse` | `azure-synapse-artifacts` |
| `databricks` | `databricks-sdk` |
| `fabric` | the `azure-core` ceiling for Fabric's runtime |
| `standalone` | `pyspark`, `delta-spark`, `pandas`, `pyarrow` |
| `adx` | Azure Data Explorer clients |
| `all` | everything above except the Fabric ceiling |

A released version installs from PyPI (`pip install 'spark-kindling[synapse]'`).
To install a local build, put the extra on a direct wheel reference:

```bash
pip install "spark-kindling[synapse] @ file://$PWD/dist/spark_kindling-<version>-py3-none-any.whl"
```

or use a release wheel URL the same way (see [setup_guide.md](../guide/setup_guide.md)). The
platform is detected at runtime; each platform module registers itself through
the `spark_kindling.platforms` entry-point group.

## Project Configuration

- Every `pyproject.toml` (root, `packages/kindling_cli`, `packages/kindling_sdk`,
  `packages/extensions/*`) is a PEP 621 `[project]` table built by `uv_build`
  (`requires = ["uv_build>=0.12.20,<0.13"]`), with `[tool.uv.build-backend]`
  naming the module and its root.
- `[project.optional-dependencies]` holds the platform extras;
  `[project.entry-points."spark_kindling.platforms"]` the platform modules.
- The root is a uv workspace: `packages/kindling_cli` and
  `packages/kindling_sdk` are members, installed editable in the dev
  environment. Extensions are not members; each pins a released
  `spark-kindling` range and is built standalone.
- `[dependency-groups] dev` is installed by default with `uv sync`.
- `uv.lock` locks the workspace. Change dependencies with `uv add` /
  `uv lock --upgrade-package <pkg>` and commit the lockfile.
- `[tool.poe.tasks]` defines build, deploy, test and release tasks.

## Verifying a Wheel's Metadata

```bash
poe build
unzip -p dist/spark_kindling-*.whl '*.dist-info/METADATA' | grep -E 'Requires-(Dist|Python)|Provides-Extra'
unzip -p dist/spark_kindling-*.whl '*.dist-info/entry_points.txt'
uvx twine check dist/*                # the README renders as the PyPI project page
```

`[project]` metadata (description, `readme`, classifiers, keywords,
`[project.urls]`) is what PyPI shows, and READMEs use absolute GitHub links so
they resolve on pypi.org.

The Fabric `azure-core` ceiling is expressed as two `Requires-Dist: azure-core`
lines, the second scoped to `extra == "fabric"`; see the comment above
`dependencies` in the root `pyproject.toml` before changing it.
