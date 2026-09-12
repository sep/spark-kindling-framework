"""Entity/config builders shared by the Event Hubs provider test modules.

The provider's tests are split by dependency: tests/unit/
test_entity_provider_eventhub.py covers config, transport and dispatch
against a mocked session, while tests/integration/
test_entity_provider_eventhub_preprocessing.py exercises the preprocessing
transforms and decoding UDFs against a real SparkSession. Both halves build
the same entity tags and connection strings, so those builders live here
rather than being duplicated.
"""

from kindling.data_entities import EntityMetadata


def _entity(tags):
    return EntityMetadata(
        entityid="stream.eventhub.test",
        name="eventhub_test",
        partition_columns=[],
        merge_columns=[],
        tags=tags,
        schema=None,
    )


def _connection_string(secret="abc123"):
    return (
        "Endpoint=sb://example.servicebus.windows.net/;"
        "SharedAccessKeyName=test;"
        f"SharedAccessKey={secret};"
    )
