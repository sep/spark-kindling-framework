# Build System

How this repo builds its wheels. Packaging is standard PEP 621 with the
`uv_build` backend; `poe build` is the entry point and the only supported way
to produce a release-shaped `dist/`.

## Build Commands

```bash
poe build                          # every wheel, into dist/

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
`uv build --wheel` once per package:

| Wheel | Source | Notes |
|---|---|---|
| `spark_kindling-<v>` | root `pyproject.toml`, module `packages/kindling` | The runtime. Contains every `platform_*.py`; platform dependencies are extras. |
| `spark_kindling_cli-<v>` | `packages/kindling_cli` | Design-time CLI, including the scaffolding templates. |
| `spark_kindling_sdk-<v>` | `packages/kindling_sdk` | Design-time platform SDK. |
| `spark_kindling_ext_<name>-<v>` | `packages/extensions/kindling_ext_<name>` | Each extension, at its own version. |

Runtime, CLI and SDK share one version (bumped together by `poe version`);
extensions version independently.

The runtime wheel is installed with the extra for its environment:

```bash
pip install 'spark-kindling[synapse]'      # azure-synapse-artifacts
pip install 'spark-kindling[databricks]'   # databricks-sdk
pip install 'spark-kindling[fabric]'       # azure-core ceiling for Fabric's runtime
pip install 'spark-kindling[standalone]'   # pyspark, delta-spark, pandas, pyarrow
pip install 'spark-kindling[adx]'          # Azure Data Explorer clients
pip install 'spark-kindling[all]'
```

The platform is detected at runtime; each platform module registers itself
through the `spark_kindling.platforms` entry-point group. The packages are not
on PyPI yet; see [setup_guide.md](../guide/setup_guide.md) for installing by
release wheel URL.

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
```

The Fabric `azure-core` ceiling is expressed as two `Requires-Dist: azure-core`
lines, the second scoped to `extra == "fabric"`; see the comment above
`dependencies` in the root `pyproject.toml` before changing it.
