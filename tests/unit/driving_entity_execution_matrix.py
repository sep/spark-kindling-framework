"""Shared driving-entity execution cases for unit tests."""

from dataclasses import dataclass
from typing import Any, FrozenSet, List, Mapping, Optional, Tuple
from unittest.mock import Mock

from kindling.data_pipes import PipeMetadata


@dataclass(frozen=True)
class DrivingEntityExecutionCase:
    """One read/skip scenario that must behave the same in both executers."""

    case_id: str
    input_entity_ids: Tuple[str, ...]
    driving_entity_ids: Optional[Tuple[str, ...]]
    use_watermark: bool
    empty_entity_ids: FrozenSet[str]
    expected_read_calls: Tuple[Tuple[str, bool], ...]
    expected_status: str


@dataclass(frozen=True)
class DrivingEntityExecutionResult:
    """Observed result from one executer adapter."""

    status: str
    read_calls: Tuple[Tuple[str, bool], ...]
    execute_kwargs: Optional[Mapping[str, Any]]
    activated: bool


DRIVING_ENTITY_EXECUTION_CASES = (
    DrivingEntityExecutionCase(
        case_id="default_first_input_watermarked",
        input_entity_ids=("source.orders", "reference.customers"),
        driving_entity_ids=None,
        use_watermark=True,
        empty_entity_ids=frozenset(),
        expected_read_calls=(
            ("source.orders", True),
            ("reference.customers", False),
        ),
        expected_status="success",
    ),
    DrivingEntityExecutionCase(
        case_id="two_declared_driving_inputs_plus_reference",
        input_entity_ids=("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
        use_watermark=True,
        empty_entity_ids=frozenset(),
        expected_read_calls=(
            ("source.orders", True),
            ("source.lines", True),
            ("reference.customers", False),
        ),
        expected_status="success",
    ),
    DrivingEntityExecutionCase(
        case_id="declared_driving_input_not_first",
        input_entity_ids=("reference.calendar", "source.orders"),
        driving_entity_ids=("source.orders",),
        use_watermark=True,
        empty_entity_ids=frozenset(),
        expected_read_calls=(
            ("reference.calendar", False),
            ("source.orders", True),
        ),
        expected_status="success",
    ),
    DrivingEntityExecutionCase(
        case_id="pipe_watermark_disabled",
        input_entity_ids=("source.orders", "source.lines"),
        driving_entity_ids=("source.orders", "source.lines"),
        use_watermark=False,
        empty_entity_ids=frozenset(),
        expected_read_calls=(
            ("source.orders", False),
            ("source.lines", False),
        ),
        expected_status="success",
    ),
    DrivingEntityExecutionCase(
        case_id="one_empty_driving_read_executes",
        input_entity_ids=("source.orders", "source.lines"),
        driving_entity_ids=("source.orders", "source.lines"),
        use_watermark=True,
        empty_entity_ids=frozenset({"source.orders"}),
        expected_read_calls=(
            ("source.orders", True),
            ("source.lines", True),
        ),
        expected_status="success",
    ),
    DrivingEntityExecutionCase(
        case_id="all_driving_reads_empty_skips",
        input_entity_ids=("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
        use_watermark=True,
        empty_entity_ids=frozenset({"source.orders", "source.lines"}),
        expected_read_calls=(
            ("source.orders", True),
            ("source.lines", True),
            ("reference.customers", False),
        ),
        expected_status="skipped",
    ),
    DrivingEntityExecutionCase(
        case_id="zero_input_pipe_executes",
        input_entity_ids=(),
        driving_entity_ids=None,
        use_watermark=True,
        empty_entity_ids=frozenset(),
        expected_read_calls=(),
        expected_status="success",
    ),
    DrivingEntityExecutionCase(
        case_id="reference_input_unavailable_executes",
        input_entity_ids=("source.orders", "reference.customers"),
        driving_entity_ids=("source.orders",),
        use_watermark=True,
        empty_entity_ids=frozenset({"reference.customers"}),
        expected_read_calls=(
            ("source.orders", True),
            ("reference.customers", False),
        ),
        expected_status="success",
    ),
)


def driving_entity_case_id(case: DrivingEntityExecutionCase) -> str:
    """Return a stable pytest id for a matrix case."""
    return case.case_id


def entity_kwarg(entity_id: str) -> str:
    """Return the normalized pipe body kwarg for an entity id."""
    return entity_id.replace(".", "_")


def build_pipe_for_case(case: DrivingEntityExecutionCase):
    """Build a PipeMetadata and mocks for one matrix case."""
    output_frame = Mock(name=f"{case.case_id}.output")
    execute = Mock(name=f"{case.case_id}.execute", return_value=output_frame)
    driving_entity_ids = (
        list(case.driving_entity_ids) if case.driving_entity_ids is not None else None
    )
    pipe = PipeMetadata(
        pipeid=case.case_id,
        name=case.case_id,
        execute=execute,
        tags={},
        input_entity_ids=list(case.input_entity_ids),
        output_entity_id=f"{case.case_id}.output",
        output_type="delta",
        use_watermark=case.use_watermark,
        driving_entity_ids=driving_entity_ids,
    )
    return pipe, execute, output_frame


def build_entity_reader_for_case(
    case: DrivingEntityExecutionCase,
    read_calls: List[Tuple[str, bool]],
):
    """Build a mock entity reader that records watermark decisions."""
    frames = {
        entity_id: Mock(name=f"{case.case_id}.{entity_kwarg(entity_id)}")
        for entity_id in case.input_entity_ids
    }

    def reader(entity, use_watermark):
        entity_id = entity.entityid
        read_calls.append((entity_id, use_watermark))
        if entity_id in case.empty_entity_ids:
            return None
        return frames[entity_id]

    return reader


def build_execution_result(
    status: str,
    read_calls: List[Tuple[str, bool]],
    execute: Mock,
    activator: Mock,
) -> DrivingEntityExecutionResult:
    """Normalize executer-specific observations into a shared result."""
    execute_kwargs = dict(execute.call_args.kwargs) if execute.call_args else None
    return DrivingEntityExecutionResult(
        status=status,
        read_calls=tuple(read_calls),
        execute_kwargs=execute_kwargs,
        activated=activator.called,
    )


def assert_driving_entity_execution_case(
    case: DrivingEntityExecutionCase,
    result: DrivingEntityExecutionResult,
) -> None:
    """Assert shared read, skip, and kwarg behavior for both executers."""
    assert result.status == case.expected_status
    assert result.read_calls == case.expected_read_calls

    if case.expected_status == "skipped":
        assert result.execute_kwargs is None
        assert result.activated is False
        return

    assert result.execute_kwargs is not None
    assert result.activated is True
    expected_kwargs = tuple(entity_kwarg(eid) for eid in case.input_entity_ids)
    assert tuple(result.execute_kwargs) == expected_kwargs
    for entity_id in case.input_entity_ids:
        value = result.execute_kwargs[entity_kwarg(entity_id)]
        if entity_id in case.empty_entity_ids:
            assert value is None
        else:
            assert value is not None
