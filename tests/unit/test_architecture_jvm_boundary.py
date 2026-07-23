"""Architecture test: py4j JVM bridge access is confined to an allowlist.

``spark._jvm`` / ``spark._jsc`` raise PySparkAttributeError on Databricks UC
shared/standard access mode clusters and Spark Connect. Any use in framework
code must either live in a platform module whose runtime guarantees a JVM
bridge, or degrade gracefully behind an AttributeError guard with a latch.

See docs/contributing/logging_tracing.md ("JVM bridge boundary").
"""

import ast
from pathlib import Path

PACKAGES_ROOT = Path(__file__).resolve().parents[2] / "packages"

BANNED_NAMES = {"_jvm", "_jsc"}

# Files allowed to touch the JVM bridge, with why.
ALLOWED_FILES = {
    "kindling/platform_standalone.py",  # standalone runtime always has classic py4j
    "kindling/platform_synapse.py",  # Synapse runtime has a JVM bridge
    "kindling/platform_databricks.py",  # hasattr-guarded probe
    "kindling/spark_trace.py",  # latched AttributeError fallbacks (mdc_context)
    "kindling/spark_log.py",  # guarded fallback to python logging
    "kindling/bootstrap.py",  # guarded get_spark_log_level
    "kindling/notebook_framework.py",  # guarded get_spark_log_level
    "kindling/entity_provider_eventhub.py",  # getattr-guarded version probe
    "kindling/features.py",  # the jvm_bridge capability probe itself
    "kindling/test_framework.py",  # builds a MagicMock _jvm, never a real bridge
}


def _jvm_references(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in BANNED_NAMES:
            yield node.lineno, node.attr
        elif isinstance(node, ast.Name) and node.id in BANNED_NAMES:
            yield node.lineno, node.id
        elif isinstance(node, ast.Constant) and node.value in BANNED_NAMES:
            yield node.lineno, node.value


def test_jvm_bridge_access_is_allowlisted():
    violations = []
    seen_allowed = set()

    for path in sorted(PACKAGES_ROOT.rglob("*.py")):
        rel = path.relative_to(PACKAGES_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
        refs = list(_jvm_references(tree))
        if not refs:
            continue
        if rel in ALLOWED_FILES:
            seen_allowed.add(rel)
            continue
        violations.extend(f"{rel}:{lineno} uses {name}" for lineno, name in refs)

    assert not violations, (
        "Direct JVM bridge access outside the allowlist (breaks Databricks UC "
        "shared/standard access mode clusters and Spark Connect). Route through "
        "PlatformService, or guard with an AttributeError latch and add the file "
        "to ALLOWED_FILES with a justification:\n" + "\n".join(violations)
    )

    stale = ALLOWED_FILES - seen_allowed
    assert (
        not stale
    ), f"Allowlist entries no longer reference the JVM bridge; prune them: {sorted(stale)}"
