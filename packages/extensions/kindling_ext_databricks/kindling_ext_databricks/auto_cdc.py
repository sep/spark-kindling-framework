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
- Untracked-column skipping for SCD1 (``scd.untracked`` on a keyed merge
  without ``scd.type``) is runner-only: keyed SCD1 is not lowered to AUTO
  CDC type 1 today, so there is nothing to map it to. If SCD1 is ever
  lowered, AUTO CDC type 1 has no untracked-column concept and rewrites
  the row on any change — that would be a divergence to document here.

Not a divergence: ``scd.tracked`` / ``scd.untracked`` under SCD2. A change
confined to columns outside the tracked set (and a sequence-only bump)
updates the current version in place in both engines; AUTO CDC does this
for columns outside ``track_history_column_list`` / inside
``track_history_except_column_list``.

``stored_as_scd_type="bitemporal"`` is deliberately NOT mapped while the
feature is in Beta (proposal decision).
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

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
    untracked_columns: Tuple[str, ...] = ()

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
    return ScdSpec(
        scd_type=scd_type,
        sequence_by=str(tags.get("scd.sequence_by", "")).strip() or None,
        source_kind=source_kind,
        delete_when=str(tags.get("scd.delete_when", "")).strip() or None,
        tracked_columns=_column_list(tags.get("scd.tracked", "")),
        untracked_columns=_column_list(tags.get("scd.untracked", "")),
    )


def _column_list(raw) -> Tuple[str, ...]:
    """Split a comma-separated column tag into literal column names."""
    return tuple(column.strip() for column in str(raw).split(",") if column.strip())


def track_history_kwargs(spec: ScdSpec) -> Dict[str, List[str]]:
    """The AUTO CDC history-tracking argument for an SCD spec.

    Both ``create_auto_cdc_flow`` and ``create_auto_cdc_from_snapshot_flow``
    accept ``track_history_column_list`` or ``track_history_except_column_list``
    (https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes,
    https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot).
    The two are alternatives, so at most one is returned. Columns outside
    the tracked set are updated in place on the current version
    (https://docs.databricks.com/aws/en/ldp/cdc), which is the runner's
    behaviour for ``scd.tracked`` / ``scd.untracked``.

    - ``scd.tracked`` -> ``track_history_column_list`` (the allow-list).
    - ``scd.untracked`` -> ``track_history_except_column_list`` with the
      untracked columns plus ``scd.sequence_by`` when set.
    - neither -> ``track_history_except_column_list=[sequence_by]`` when
      ``scd.sequence_by`` is set (the sequence column is ordering
      authority, not content, so a sequence-only bump updates in place),
      otherwise nothing (every column is tracked).
    """
    if spec.tracked_columns:
        return {"track_history_column_list": list(spec.tracked_columns)}
    except_columns = list(spec.untracked_columns)
    if spec.sequence_by and spec.sequence_by not in except_columns:
        except_columns.append(spec.sequence_by)
    if except_columns:
        return {"track_history_except_column_list": list(dict.fromkeys(except_columns))}
    return {}


def validate_scd_spec(spec: ScdSpec, merge_columns: List[str]) -> List[Tuple[str, str]]:
    """Statically checkable AUTO CDC mapping requirements.

    Returns ``(issue_code, reason)`` pairs; empty when mappable.
    """
    issues: List[Tuple[str, str]] = []
    if spec.scd_type != "2":
        issues.append(
            (
                "scd_type_unsupported",
                f"scd.type '{spec.scd_type}' has no AUTO CDC mapping — only "
                "SCD Type 2 is declarable in kindling (core registration "
                "enforces scd.type='2'; bitemporal is deliberately excluded "
                "while in Beta)",
            )
        )
    if spec.is_snapshot and spec.delete_when:
        issues.append(
            (
                "scd_delete_when_snapshot_unsupported",
                "scd.delete_when cannot be used with snapshot AUTO CDC — "
                "a snapshot expresses deletion by absence",
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
    if spec.tracked_columns and spec.untracked_columns:
        issues.append(
            (
                "scd_tracked_untracked_exclusive",
                "scd.tracked and scd.untracked are mutually exclusive — declare "
                "the allow-list or the exclude-list, not both (AUTO CDC takes "
                "only one of track_history_column_list / "
                "track_history_except_column_list)",
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
