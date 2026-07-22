"""
Unit tests for the sugar declaration helpers — the reference
implementation of the declaration convention
(docs/contributing/declaration_conventions.md): an annotation only sets
canonical tags as defaults, explicit tags win, and all behavior lives in
the tags.
"""

from unittest.mock import Mock

import pytest
from kindling.data_entities import DataEntities


@pytest.fixture(autouse=True)
def fake_registry():
    registry = Mock()
    DataEntities.deregistry = registry
    yield registry
    DataEntities.reset()


def _declared_tags(registry):
    _, kwargs = registry.register_entity.call_args
    return kwargs["tags"]


def _declare(helper, tags=None, **extra):
    helper(
        entityid="gold.summary",
        name="summary",
        merge_columns=extra.pop("merge_columns", []),
        schema=None,
        tags=tags,
        **extra,
    )


class TestDerivedEntity:
    def test_sets_dataset_kind_default(self, fake_registry):
        _declare(DataEntities.derived_entity)
        assert _declared_tags(fake_registry)["dataset.kind"] == "derived"

    def test_replace_keys_list_becomes_tag(self, fake_registry):
        _declare(DataEntities.derived_entity, replace_keys=["run_id", "site"])
        assert _declared_tags(fake_registry)["derived.replace_keys"] == "run_id,site"

    def test_replace_keys_string_passes_through(self, fake_registry):
        _declare(DataEntities.derived_entity, replace_keys="run_id")
        assert _declared_tags(fake_registry)["derived.replace_keys"] == "run_id"

    def test_explicit_tags_win_over_defaults(self, fake_registry):
        _declare(
            DataEntities.derived_entity,
            replace_keys=["run_id"],
            tags={"derived.replace_keys": "site"},
        )
        tags = _declared_tags(fake_registry)
        assert tags["derived.replace_keys"] == "site"
        assert tags["dataset.kind"] == "derived"

    def test_other_tags_preserved(self, fake_registry):
        _declare(DataEntities.derived_entity, tags={"provider_type": "delta"})
        assert _declared_tags(fake_registry)["provider_type"] == "delta"


class TestInsertOnlyEntity:
    def test_sets_write_mode_default(self, fake_registry):
        _declare(DataEntities.insert_only_entity, merge_columns=["event_id"])
        assert _declared_tags(fake_registry)["write.mode"] == "insert"

    def test_explicit_write_mode_wins(self, fake_registry):
        # The annotation proposes, the tag disposes.
        _declare(
            DataEntities.insert_only_entity,
            merge_columns=["event_id"],
            tags={"write.mode": "merge"},
        )
        assert _declared_tags(fake_registry)["write.mode"] == "merge"


class TestNoBehaviorInAnnotation:
    def test_derived_delegates_to_registration_validation(self, fake_registry):
        """The helper adds tags and delegates — canonical tag validation
        (e.g. derived rejects scd.type) still runs in register_entity."""
        fake_registry.register_entity.side_effect = ValueError("scd.type does not apply")
        with pytest.raises(ValueError, match="scd.type does not apply"):
            _declare(DataEntities.derived_entity, tags={"scd.type": "2"})


class TestReplaceKeysTypeValidation:
    def test_set_rejected_with_clear_error(self, fake_registry):
        with pytest.raises(ValueError, match="list/tuple of column names, got set"):
            _declare(DataEntities.derived_entity, replace_keys={"run_id", "site"})

    def test_non_string_elements_rejected(self, fake_registry):
        with pytest.raises(ValueError, match="list/tuple of column names"):
            _declare(DataEntities.derived_entity, replace_keys=[1, 2])

    def test_tuple_accepted(self, fake_registry):
        _declare(DataEntities.derived_entity, replace_keys=("run_id", "site"))
        assert _declared_tags(fake_registry)["derived.replace_keys"] == "run_id,site"
