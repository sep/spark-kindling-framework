"""SCD declared-flow -> AUTO CDC mapping (proposal Phase 5).

The SCD2-as-a-declared-flow prerequisite (#159) made SCD intent fully
declarable — ``scd.sequence_by`` / ``scd.source_kind`` / ``scd.delete_when``
tags — with the runner's merge machinery as ONE compilation target. This
module is the other target: Databricks AUTO CDC flows, per the current
Lakeflow API (``create_auto_cdc_flow`` / ``create_auto_cdc_from_snapshot_
flow`` — AUTO CDC is explicitly unsupported on OSS Spark SDP, which is why
this lives in the adapter and why core capability-gating fails ``scd.*``
tags against ``OSS_SDP``).

Documented divergences from the runner engine (dual-engine parity
criterion):

- SCD2 history columns are AUTO CDC's ``__START_AT``/``__END_AT``, not the
  runner's ``__effective_from``/``__effective_to``/``__is_current``. The
  entity's declared schema (which describes the runner shape) is therefore
  NOT passed to the streaming table.
- Snapshot ordering comes from pipeline ingestion order (AUTO CDC FROM
  SNAPSHOT), not from ``scd.sequence_by`` — the snapshot API takes no
  sequencing column.
- Out-of-order change-feed rows are RECONCILED into history (the reason
  AUTO CDC exists) rather than ignored under the runner's strictly-later
  rule.
- The ``scd.current_entity_id`` current-view companion is runner-only
  machinery; in SDP it should become an ordinary declared view (tracked
  separately).

``stored_as_scd_type="bitemporal"`` is deliberately NOT mapped while the
feature is in Beta (proposal decision).
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

#: Suffix for the pipeline-scoped view holding the pipe's change/snapshot
#: source — the AUTO CDC flow reads it by name.
SCD_SOURCE_SUFFIX = "__scd_source"


@dataclass(frozen=True)
class ScdSpec:
    """The subset of #159's SCD tag vocabulary that AUTO CDC consumes."""

    scd_type: str
    sequence_by: Optional[str]
    source_kind: str  # "snapshot" | "change_feed"
    delete_when: Optional[str]
    tracked_columns: Tuple[str, ...]

    @property
    def is_snapshot(self) -> bool:
        return self.source_kind == "snapshot"


def scd_spec_from_tags(tags) -> Optional[ScdSpec]:
    """Parse the SCD tags (same vocabulary as kindling's
    ``scd_config_from_tags``, minus runner-only knobs). Returns ``None``
    for non-SCD entities."""
    tags = tags or {}
    scd_type = str(tags.get("scd.type", "")).strip()
    if not scd_type:
        return None
    close_on_missing = str(tags.get("scd.close_on_missing", "")).strip().lower() == "true"
    source_kind = str(tags.get("scd.source_kind", "")).strip().lower() or (
        "snapshot" if close_on_missing else "change_feed"
    )
    tracked_raw = str(tags.get("scd.tracked_columns", "")).strip()
    return ScdSpec(
        scd_type=scd_type,
        sequence_by=str(tags.get("scd.sequence_by", "")).strip() or None,
        source_kind=source_kind,
        delete_when=str(tags.get("scd.delete_when", "")).strip() or None,
        tracked_columns=tuple(
            column.strip() for column in tracked_raw.split(",") if column.strip()
        ),
    )


def validate_scd_spec(spec: ScdSpec, merge_columns: List[str]) -> List[Tuple[str, str]]:
    """Statically checkable AUTO CDC mapping requirements.

    Returns ``(issue_code, reason)`` pairs; empty when mappable.
    """
    issues: List[Tuple[str, str]] = []
    if spec.scd_type not in ("1", "2"):
        issues.append(
            (
                "scd_type_unsupported",
                f"scd.type '{spec.scd_type}' has no AUTO CDC mapping "
                "(bitemporal is deliberately excluded while in Beta)",
            )
        )
    if not spec.is_snapshot and not spec.sequence_by:
        issues.append(
            (
                "scd_sequence_by_required",
                "the AUTO CDC change-feed mapping requires scd.sequence_by — "
                "ordering authority must come from the data (declare the "
                "tag, or use scd.source_kind: snapshot)",
            )
        )
    if not merge_columns:
        issues.append(
            (
                "scd_keys_required",
                "AUTO CDC requires key columns; the entity declares no merge_columns",
            )
        )
    return issues
