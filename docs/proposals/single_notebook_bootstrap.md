# Research: Publishing Kindling as a Single Bootstrap Notebook

**Status:** Research / Evaluation
**Date:** 2026-02-12
**Author:** AI-assisted analysis

## Summary

Evaluation of what it would take to publish the Kindling framework as a single notebook that could be run in a notebook environment to bootstrap the framework, without requiring a separate wheel download from Azure Storage.

## Package Metrics

| Metric | Value |
|---|---|
| Python files | 42 |
| Total lines of code | 23,659 |
| Total size (source) | ~860 KB |
| Largest file | `platform_fabric.py` (2,558 lines) |
| Top 7 files (>1,000 lines) | 53% of total code (12,528 lines) |
| Runtime dependencies | 12 packages |
| Structure | Flat (all 42 `.py` files in one directory) |

### Runtime Dependencies

The framework requires these packages at runtime — they cannot be embedded in a notebook:

- `pyspark >=3.4.0`
- `delta-spark >=2.4.0`
- `pandas >=2.0.0`
- `pyarrow >=12.0.0`
- `injector >=0.20.1`
- `dynaconf >=3.1.0`
- `pyyaml >=6.0`
- `packaging >=23.0`
- `blinker >=1.6.0`
- `azure-storage-blob >=12.14.0`
- `azure-storage-file-datalake >=12.14.0`
- `azure-identity >=1.15.0`
- `azure-core >=1.30.0,<1.31.0`
- `azure-synapse-artifacts ==0.17.0`
- `databricks-sdk >=0.12.0`

## Platform Notebook Limits

| Limit | Databricks | Fabric | Synapse |
|---|---|---|---|
| **Max cells per notebook** | 10,000 (documented, fixed) | Not explicitly documented | Not explicitly documented |
| **Max notebook file size** | 500 MB (workspace files) | 100 MB single file; 500 MB resource folder | Not explicitly documented |
| **Per-cell size limit** | Not explicitly documented | Not explicitly documented | Not explicitly documented |
| **Notebook format** | `.py` / `.ipynb` / `.dbc` | `.Notebook` (JSON); `.ipynb` for import | `.json`-based (Synapse Studio format) |

**At ~860 KB of source / 42 modules, the package is well within all documented limits on every platform.**

### Sources

- Databricks: [Resource limits](https://learn.microsoft.com/en-us/azure/databricks/resources/limits) — "Notebook > Cells > 10,000 per Notebook (Fixed)"
- Fabric: [How to use notebooks](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook) — resource storage 500 MB / 100 files; no explicit cell count cap documented
- Synapse: [Create, develop, and maintain notebooks](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-development-using-notebooks) — no explicit size/cell limits documented

## Approaches Evaluated

### 1. `%%writefile` Per Module (Most Readable)

Each of the 42 modules gets a cell using `%%writefile` to write the `.py` file to a temp directory, followed by a final cell that adds the directory to `sys.path` and imports kindling.

| Aspect | Assessment |
|---|---|
| Cells needed | ~44 (42 writefile + 1 setup + 1 import) |
| Readability | High — code is visible and editable inline |
| Portability | Works on all 3 platforms (`%%writefile` is standard IPython) |
| Drawbacks | Verbose; writes to local disk; doesn't install runtime dependencies |

### 2. Embedded Base64 Wheel (Most Robust) — RECOMMENDED

Embed the pre-built `.whl` file as a base64 string in a single cell, decode it to `/tmp`, then `%pip install /tmp/kindling-*.whl`.

```python
# Cell 1: Decode wheel
import base64, tempfile, os
WHEEL_B64 = "..."  # ~1.1 MB base64 string
wheel_path = os.path.join(tempfile.mkdtemp(), "kindling-0.2.0-py3-none-any.whl")
with open(wheel_path, "wb") as f:
    f.write(base64.b64decode(WHEEL_B64))
```

```python
# Cell 2: Install
%pip install /tmp/.../kindling-0.2.0-py3-none-any.whl
```

```python
# Cell 3: Use kindling
from kindling.bootstrap import bootstrap_framework
```

| Aspect | Assessment |
|---|---|
| Cells needed | 2–3 |
| Readability | Low — base64 blob is opaque |
| Portability | Works on all 3 platforms |
| Pros | Handles all inter-module imports; proper package namespace; pip resolves dependencies |
| Drawbacks | Wheel must be re-encoded on each release; ~1.1 MB base64 string in a cell |

### 3. Dynamic `types.ModuleType` In-Memory Injection

Use `exec()` and `types.ModuleType` to create modules entirely in memory without touching disk.

| Aspect | Assessment |
|---|---|
| Cells needed | 1–2 |
| Portability | Theoretically universal |
| Drawbacks | Fragile; complex import resolution across 42 inter-dependent modules; circular dependency ordering is extremely hard; debugging nightmares |

**Not recommended.**

### 4. Single Giant Code Cell (Flattened)

Concatenate all 42 files into one cell, flattening namespaces.

| Aspect | Assessment |
|---|---|
| Cells needed | 1 |
| Drawbacks | **Breaks completely** — name collisions, lost module boundaries, import statements fail, ~24K lines in one cell is unusable |

**Not viable.**

## Critical Challenges

### 1. Runtime Dependencies Cannot Be Embedded
All approaches still need `%pip install` (or environment-level packages) for the 12+ runtime dependencies. A single notebook cannot be fully self-contained without also bundling dependencies.

### 2. Inter-Module Imports
The 42 modules import from each other extensively (e.g., `from kindling.bootstrap import ...`). Any approach must create a proper `kindling` package namespace with correct module resolution.

### 3. Platform Service Detection
The framework auto-detects its platform at import time (Fabric vs Synapse vs Databricks). A notebook bootstrap must ensure this detection still works correctly.

### 4. Maintenance Burden
Every code change requires regenerating the notebook. This is a **build artifact**, not a source artifact — it should be generated as part of the release pipeline, not hand-maintained.

### 5. Notebook Format Differences
Fabric uses `.Notebook` (JSON), Databricks uses `.py`/`.ipynb`, Synapse uses its own JSON format. A "universal" notebook needs either:
- Platform-specific export targets, or
- `.ipynb` as the lowest common denominator (importable by all three platforms)

## Recommendation

**The embedded wheel approach (option 2) is the most viable**, and it's essentially what the existing bootstrap script already does — download a wheel from storage and `pip install` it. The single-notebook variant simply embeds the wheel inline rather than downloading it.

**However, this offers marginal value over the current approach** (download wheel from Azure Storage), since both require a network connection for the 12+ third-party dependencies anyway. The real win would be for **air-gapped environments**, but those would also need the dependencies bundled — which is a separate, larger effort.

### When This Would Be Valuable

- **Demos / quickstart**: Share a single notebook (or gist) that sets up the entire framework
- **Air-gapped/restricted networks**: Environments that can't reach Azure Storage (would also need dependency bundling)
- **Education/training**: Self-contained workshop material
- **Offline evaluation**: Prospects evaluating the framework without configuring storage

## Effort Estimate

| Task | Effort |
|---|---|
| Build script to encode wheel into notebook cell | ~2 hours |
| Generate platform-specific notebook formats (`.ipynb`, `.Notebook`) | ~4 hours |
| Handle dependency bundling (optional, for air-gap) | ~8–16 hours |
| Test across all 3 platforms | ~4 hours |
| Add to `poe` task pipeline | ~1 hour |
| **Total (without air-gap)** | **~11 hours** |
| **Total (with air-gap dependency bundling)** | **~27 hours** |

## Comparison to Current Bootstrap

| Aspect | Current (wheel download) | Single notebook (embedded wheel) |
|---|---|---|
| Network required | Yes (Azure Storage + PyPI) | Yes (PyPI for dependencies) |
| Setup complexity | Configure storage path + bootstrap script | Run the notebook |
| Versioning | Wheel version in storage | Notebook must be regenerated per release |
| Air-gap support | No | Partial (still needs dependencies) |
| Code visibility | None (installed as wheel) | None (base64 blob) or Full (`%%writefile`) |
| Share-ability | Requires infrastructure | Copy a single file |
