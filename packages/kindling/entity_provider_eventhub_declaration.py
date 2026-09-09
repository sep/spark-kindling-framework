"""Declaration-only helpers for Event Hub streaming sources."""

from typing import Any, Mapping, Optional

from .entity_provider import (
    PreprocessingSpec,
    SourceValidationIssue,
    StreamingSourceSpec,
)

TRANSPORT_AUTO = "auto"
TRANSPORT_EVENTHUBS = "eventhubs"
TRANSPORT_KAFKA = "kafka"
TRANSPORTS = frozenset({TRANSPORT_AUTO, TRANSPORT_EVENTHUBS, TRANSPORT_KAFKA})

PREPROCESS_MODE_NAMES = frozenset({"avro", "kafka"})

DECLARABLE_SUPPORTED_TAGS = (
    "provider.eventhub.connectionString",
    "provider.eventhub.name",
    "provider.eventhub.consumerGroup",
    "provider.transport",
    "provider.startingPosition",
    "provider.maxEventsPerTrigger",
    "provider.operationTimeout",
    "provider.kafka.*",
    "provider.preprocess",
    "provider.amqp_headers",
)


def resolve_transport(provider_config: Mapping[str, Any], platform: str) -> str:
    """Resolve the Event Hub transport without touching Spark or secrets."""
    configured_transport = (
        str(provider_config.get("transport", TRANSPORT_AUTO) or TRANSPORT_AUTO).strip().lower()
    )

    if configured_transport not in TRANSPORTS:
        raise ValueError("Event Hub provider transport must be one of: auto, eventhubs, kafka")

    if configured_transport != TRANSPORT_AUTO:
        return configured_transport

    if platform == "databricks":
        return TRANSPORT_KAFKA

    return TRANSPORT_EVENTHUBS


def parse_connection_string(connection_string: str) -> dict:
    """Parse the non-secret shape of an Event Hub connection string."""
    parts: dict[str, str] = {}
    for segment in connection_string.split(";"):
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        parts[key.strip()] = value.strip()

    required = ("Endpoint=", "SharedAccessKeyName=", "SharedAccessKey=")
    if not all(token in connection_string for token in required):
        raise ValueError("Event Hub connection string missing required segments")

    return parts


def with_entity_path(connection_string: str, eventhub_name: str) -> str:
    """Append EntityPath when the Event Hub connection string omits it."""
    if "EntityPath=" in connection_string:
        return connection_string
    return f"{connection_string.rstrip(';')};EntityPath={eventhub_name}"


def build_streaming_source_spec(
    entity_metadata,
    config: Mapping[str, Any],
    *,
    platform: str,
) -> StreamingSourceSpec:
    """Build an inert, secret-safe Lakeflow declaration spec."""
    tags = entity_metadata.tags or {}
    transport, issues = _transport_and_issues(config, platform)
    connection_parts, connection_issues = _connection_parts_and_issues(config)
    issues.extend(connection_issues)

    eventhub_name = str(config.get("eventhub.name", "") or "").strip()
    issues.extend(_eventhub_name_issues(eventhub_name))
    issues.extend(_starting_position_issues(config))
    preprocess_mode, preprocess_issues = _preprocess_mode_and_issues(config)
    issues.extend(preprocess_issues)
    issues.extend(_integer_option_issues(config))

    return StreamingSourceSpec(
        provider_type=str(tags.get("provider_type", "eventhub")),
        source_format=transport or "unknown",
        source_identity=_source_identity(eventhub_name, connection_parts),
        supported_option_names=DECLARABLE_SUPPORTED_TAGS,
        applied_option_names=_applied_option_names(tags),
        preprocessing=_preprocessing_spec(config, preprocess_mode),
        validation_issues=tuple(issues),
    )


def _issue(tag: str, constraint: str, remediation: str = "") -> SourceValidationIssue:
    return SourceValidationIssue(tag=tag, constraint=constraint, remediation=remediation)


def _transport_and_issues(
    config: Mapping[str, Any], platform: str
) -> tuple[str, list[SourceValidationIssue]]:
    try:
        transport = resolve_transport(config, platform)
    except ValueError:
        return "", [
            _issue(
                "provider.transport",
                "must be one of: auto, eventhubs, kafka",
                "set provider.transport to kafka for Lakeflow declarations",
            )
        ]

    if transport == TRANSPORT_EVENTHUBS:
        return transport, [
            _issue(
                "provider.transport",
                "the eventhubs transport cannot run in Lakeflow",
                "set provider.transport to kafka or configure "
                "kindling.platform.name as databricks so auto resolves to kafka",
            )
        ]

    return transport, []


def _connection_parts_and_issues(
    config: Mapping[str, Any],
) -> tuple[dict[str, str], list[SourceValidationIssue]]:
    connection_string = str(config.get("eventhub.connectionString", "") or "")
    if not connection_string:
        return {}, [
            _issue(
                "provider.eventhub.connectionString",
                "is required",
                "provide the Event Hub connection string through a secret-backed tag",
            )
        ]

    try:
        return parse_connection_string(connection_string), []
    except ValueError:
        return {}, [
            _issue(
                "provider.eventhub.connectionString",
                "must include endpoint, access key name, and access key segments",
                "use a complete Event Hub connection string",
            )
        ]


def _eventhub_name_issues(eventhub_name: str) -> list[SourceValidationIssue]:
    if eventhub_name:
        return []
    return [
        _issue(
            "provider.eventhub.name",
            "is required for Kafka declarative reads",
            "set the Event Hub name explicitly",
        )
    ]


def _starting_position_issues(config: Mapping[str, Any]) -> list[SourceValidationIssue]:
    starting_position = str(config.get("startingPosition", "latest") or "latest")
    if starting_position in {"earliest", "latest"}:
        return []
    return [
        _issue(
            "provider.startingPosition",
            "Kafka transport supports only earliest and latest",
            "set provider.startingPosition to earliest or latest",
        )
    ]


def _preprocess_mode_and_issues(
    config: Mapping[str, Any],
) -> tuple[str, list[SourceValidationIssue]]:
    preprocess = config.get("preprocess")
    preprocess_mode = str(preprocess).strip() if preprocess else ""
    if not preprocess_mode or preprocess_mode in PREPROCESS_MODE_NAMES:
        return preprocess_mode, []
    return preprocess_mode, [
        _issue(
            "provider.preprocess",
            "must be one of: avro, kafka",
            "remove provider.preprocess or select a supported mode",
        )
    ]


def _integer_option_issues(config: Mapping[str, Any]) -> list[SourceValidationIssue]:
    issues = []
    for tag_name, config_key in (
        ("provider.maxEventsPerTrigger", "maxEventsPerTrigger"),
        ("provider.operationTimeout", "operationTimeout"),
    ):
        value = config.get(config_key)
        if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
            issues.append(
                _issue(
                    tag_name,
                    "must be an integer",
                    "set an integer millisecond/event count value",
                )
            )
    return issues


def _source_identity(eventhub_name: str, connection_parts: Mapping[str, str]) -> str:
    namespace_host = str(connection_parts.get("Endpoint", "")).replace("sb://", "", 1).rstrip("/")
    source_identity = eventhub_name or "<missing event hub name>"
    if namespace_host:
        source_identity = f"{source_identity}@{namespace_host}"
    return source_identity


def _applied_option_names(tags: Mapping[str, Any]) -> tuple[str, ...]:
    applied = []
    for tag_name in DECLARABLE_SUPPORTED_TAGS:
        if tag_name.endswith(".*"):
            prefix = tag_name[:-1]
            applied.extend(sorted(key for key in tags if key.startswith(prefix)))
        elif tag_name in tags:
            applied.append(tag_name)
    return tuple(applied)


def _preprocessing_spec(
    config: Mapping[str, Any], preprocess_mode: str
) -> Optional[PreprocessingSpec]:
    if not preprocess_mode:
        return None
    return PreprocessingSpec(
        mode=preprocess_mode,
        amqp_headers=bool(config.get("amqp_headers")),
        kafka_headers_included=bool(config.get("kafka.includeHeaders")),
    )
