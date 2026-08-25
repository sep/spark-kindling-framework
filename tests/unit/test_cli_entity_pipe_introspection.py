"""Unit tests for `kindling entity list`, `kindling entity tags`, and
`kindling pipeline show`.

Mirrors the mocking pattern in tests/unit/test_cli_entity_commands.py:
app.py is a trivial no-op `initialize()` stub, and
`kindling.injection.GlobalInjector.get` is monkeypatched directly so no real
Spark/bootstrap ever runs.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from kindling_cli.cli import (
    _build_tag_view,
    _raw_registration_tags,
    _resolve_tag_provenance,
    cli,
)


@pytest.fixture(autouse=True)
def _mock_bootstrap_app(monkeypatch):
    """The CLI now always calls a real `initialize_framework()` via
    `_bootstrap_app` before loading app.py; these tests mock
    `GlobalInjector.get` narrowly and don't want a real bootstrap call."""
    monkeypatch.setattr("kindling_cli.cli._bootstrap_app", lambda *a, **kw: None)


def _write_app(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "def initialize(env=None, config_dir=None):\n    return None\n", encoding="utf-8"
    )
    return path


def _make_entity(entity_id: str, tags=None, merge_columns=None, schema=None):
    return SimpleNamespace(
        entityid=entity_id,
        tags=tags or {},
        merge_columns=merge_columns or [],
        schema=schema,
    )


def _make_pipe(
    pipeid: str,
    tags=None,
    input_entity_ids=None,
    output_entity_id="gold.summary",
    output_type="delta",
    name=None,
):
    return SimpleNamespace(
        pipeid=pipeid,
        name=name or pipeid,
        tags=tags or {},
        input_entity_ids=input_entity_ids or [],
        output_entity_id=output_entity_id,
        output_type=output_type,
    )


class _FakeEntityRegistry:
    """Plain fake (not Mock(spec=...)) so `_raw_params` is freely settable --
    Mock's spec restriction would otherwise block reads/writes of a private
    attribute that isn't part of the DataEntityRegistry ABC surface."""

    def __init__(self, entities, raw_params=None):
        self._entities = entities
        self._raw_params = raw_params or {}

    def get_entity_definition(self, name):
        return self._entities.get(name)

    def get_entity_ids(self):
        return list(self._entities.keys())


class _FakePipeRegistry:
    def __init__(self, pipes, raw_params=None):
        self._pipes = pipes
        self._raw_params = raw_params or {}

    def get_pipe_definition(self, name):
        return self._pipes.get(name)

    def get_pipe_ids(self):
        return list(self._pipes.keys())


def _patch_registry(monkeypatch, registry_type, instance):
    def fake_get(service_type):
        if service_type is registry_type:
            return instance
        raise AssertionError(f"unexpected service: {service_type!r}")

    monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)


# ---------------------------------------------------------------------------
# _resolve_tag_provenance / _build_tag_view (pure logic)
# ---------------------------------------------------------------------------


class TestResolveTagProvenance:
    def test_no_overrides_keeps_literal_provenance(self):
        literal = {"provider_type": "delta"}
        final, provenance = _resolve_tag_provenance(
            literal, "bronze.orders", None, None, None, "bytag", "idglob", "exact"
        )
        assert final == literal
        assert provenance == {"provider_type": "literal tags="}

    def test_bytag_override_relabels_changed_key(self):
        literal = {"tier": "bronze"}
        bytag_section = {"tier": {"bronze": {"tags": {"provider.table_catalog": "dev_bronze"}}}}
        final, provenance = _resolve_tag_provenance(
            literal,
            "bronze.orders",
            bytag_section,
            None,
            None,
            "dataentities-bytag:",
            "idglob",
            "exact",
        )
        assert final["provider.table_catalog"] == "dev_bronze"
        assert final["tier"] == "bronze"
        assert provenance["provider.table_catalog"] == "dataentities-bytag:"
        assert provenance["tier"] == "literal tags="

    def test_idglob_wins_over_bytag_on_conflict(self):
        literal = {"tier": "gold"}
        bytag_section = {"tier": {"gold*": {"tags": {"schema.drift": "fail"}}}}
        idglob_section = {"bronze.orders": {"tags": {"schema.drift": "warn"}}}
        final, provenance = _resolve_tag_provenance(
            literal,
            "bronze.orders",
            bytag_section,
            idglob_section,
            None,
            "dataentities-bytag:",
            "dataentities: bronze.orders",
            "entity_tags: bronze.orders",
        )
        assert final["schema.drift"] == "warn"
        assert provenance["schema.drift"] == "dataentities: bronze.orders"

    def test_exact_overrides_win_last(self):
        literal = {"provider.table_name": "orders"}
        exact = {"provider.table_name": "@secret:kv/orders-table"}
        final, provenance = _resolve_tag_provenance(
            literal,
            "bronze.orders",
            None,
            None,
            exact,
            "bytag",
            "idglob",
            "entity_tags: bronze.orders",
        )
        assert final["provider.table_name"] == "@secret:kv/orders-table"
        assert provenance["provider.table_name"] == "entity_tags: bronze.orders"

    def test_exact_override_matching_existing_value_keeps_prior_provenance(self):
        literal = {"a": "same"}
        exact = {"a": "same"}
        final, provenance = _resolve_tag_provenance(
            literal, "x", None, None, exact, "bytag", "idglob", "exact"
        )
        assert final["a"] == "same"
        assert provenance["a"] == "literal tags="


class TestBuildTagView:
    def test_prefers_live_value_but_uses_computed_for_secret_detection(self):
        final_tags = {"provider.table_name": "resolved-value"}
        computed_tags = {"provider.table_name": "@secret:kv/orders-table"}
        provenance = {"provider.table_name": "entity_tags: bronze.orders"}

        view = _build_tag_view(final_tags, computed_tags, provenance, reveal_secrets=False)

        assert view["provider.table_name"]["value"] == "<secret: kv/orders-table>"
        assert view["provider.table_name"]["source"] == "entity_tags: bronze.orders"

    def test_reveal_secrets_shows_plaintext(self):
        final_tags = {"k": "@secret:scope:key"}
        view = _build_tag_view(final_tags, final_tags, {}, reveal_secrets=True)
        assert view["k"]["value"] == "@secret:scope:key"

    def test_non_secret_values_pass_through_unchanged(self):
        final_tags = {"provider_type": "delta"}
        view = _build_tag_view(final_tags, final_tags, {"provider_type": "literal tags="}, False)
        assert view["provider_type"] == {"value": "delta", "source": "literal tags="}

    def test_missing_provenance_falls_back_to_resolved(self):
        final_tags = {"mystery": "value"}
        view = _build_tag_view(final_tags, {}, {}, False)
        assert view["mystery"]["source"] == "resolved"


class TestRawRegistrationTags:
    def test_reads_from_raw_params(self):
        registry = SimpleNamespace(_raw_params={"bronze.orders": {"tags": {"tier": "bronze"}}})
        assert _raw_registration_tags(registry, "bronze.orders", {}) == {"tier": "bronze"}

    def test_falls_back_when_raw_params_absent(self):
        registry = SimpleNamespace()
        assert _raw_registration_tags(registry, "bronze.orders", {"tier": "bronze"}) == {
            "tier": "bronze"
        }

    def test_falls_back_when_entity_not_in_raw_params(self):
        registry = SimpleNamespace(_raw_params={})
        assert _raw_registration_tags(registry, "bronze.orders", {"tier": "bronze"}) == {
            "tier": "bronze"
        }


# ---------------------------------------------------------------------------
# kindling entity list
# ---------------------------------------------------------------------------


class TestEntityList:
    def test_lists_entities_without_tags_flag(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        registry = _FakeEntityRegistry(
            {
                "bronze.orders": _make_entity("bronze.orders", tags={"provider_type": "delta"}),
                "silver.orders": _make_entity("silver.orders", tags={"provider_type": "view"}),
            }
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "list", "--app", str(app_path)])

        assert result.exit_code == 0, result.output
        assert "bronze.orders" in result.output
        assert "silver.orders" in result.output
        assert "(delta)" in result.output
        assert "(view)" in result.output

    def test_tags_flag_adds_tags_column(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        registry = _FakeEntityRegistry(
            {"bronze.orders": _make_entity("bronze.orders", tags={"tier": "bronze"})}
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "list", "--app", str(app_path), "--tags"])

        assert result.exit_code == 0, result.output
        assert "tier=bronze" in result.output

    def test_tags_are_redacted_when_secret(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        registry = _FakeEntityRegistry(
            {
                "bronze.orders": _make_entity(
                    "bronze.orders", tags={"provider.token": "@secret:kv/token"}
                )
            }
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "list", "--app", str(app_path), "--tags"])

        assert result.exit_code == 0, result.output
        assert "<secret: kv/token>" in result.output
        assert "@secret:kv/token" not in result.output

    def test_tags_are_redacted_when_secret_already_resolved(self, monkeypatch):
        """A live SecretProvider resolves `@secret:` tags to plaintext at
        registration time (see data_entities.py), so the live tag value
        never looks like a reference by the time `entity list` reads it.
        Redaction must fall back to the never-resolved raw registration
        tags, not just re-check the live value."""
        from kindling.data_entities import DataEntityRegistry

        registry = _FakeEntityRegistry(
            {
                "bronze.orders": _make_entity(
                    "bronze.orders", tags={"provider.token": "sk-live-plaintext-value"}
                )
            },
            raw_params={"bronze.orders": {"tags": {"provider.token": "@secret:kv/token"}}},
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "list", "--app", str(app_path), "--tags"])

        assert result.exit_code == 0, result.output
        assert "<secret: kv/token>" in result.output
        assert "sk-live-plaintext-value" not in result.output

    def test_no_entities_message(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        _patch_registry(monkeypatch, DataEntityRegistry, _FakeEntityRegistry({}))

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "list", "--app", str(app_path)])

        assert result.exit_code == 0, result.output
        assert "No entities registered." in result.output

    def test_json_output(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        registry = _FakeEntityRegistry(
            {"bronze.orders": _make_entity("bronze.orders", tags={"provider_type": "delta"})}
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "list", "--app", str(app_path), "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["entities"] == [{"entity_id": "bronze.orders", "provider_type": "delta"}]


# ---------------------------------------------------------------------------
# kindling entity tags
# ---------------------------------------------------------------------------


class TestEntityTagsCommand:
    def test_unknown_entity_fails(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        _patch_registry(monkeypatch, DataEntityRegistry, _FakeEntityRegistry({}))

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli, ["entity", "tags", "missing.entity", "--app", str(app_path)]
            )

        assert result.exit_code != 0
        assert "not registered" in result.output

    def test_shows_literal_tags_with_provenance(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        entity = _make_entity("bronze.orders", tags={"provider_type": "delta"})
        registry = _FakeEntityRegistry(
            {"bronze.orders": entity},
            raw_params={"bronze.orders": {"tags": {"provider_type": "delta"}}},
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["entity", "tags", "bronze.orders", "--app", str(app_path)])

        assert result.exit_code == 0, result.output
        assert "provider_type" in result.output
        assert "delta" in result.output
        assert "literal tags=" in result.output

    def test_entity_tags_yaml_section_provenance_and_redaction(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        # Live/authoritative value: framework already resolved the secret
        # (simulating a real SecretProvider succeeding during bootstrap).
        entity = _make_entity(
            "bronze.orders",
            tags={"provider_type": "delta", "provider.table_name": "resolved-table-value"},
        )
        registry = _FakeEntityRegistry(
            {"bronze.orders": entity},
            raw_params={"bronze.orders": {"tags": {"provider_type": "delta"}}},
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "entity_tags:\n"
                "  bronze.orders:\n"
                "    provider.table_name: '@secret:kv/orders-table'\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                cli, ["entity", "tags", "bronze.orders", "--app", str(app_path), "--json"]
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["tags"]["provider.table_name"] == "<secret: kv/orders-table>"
        assert payload["provenance"]["provider.table_name"] == "entity_tags: bronze.orders"

    def test_reveal_secrets_prints_plaintext_with_warning(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        entity = _make_entity(
            "bronze.orders", tags={"provider.table_name": "@secret:kv/orders-table"}
        )
        registry = _FakeEntityRegistry(
            {"bronze.orders": entity},
            raw_params={"bronze.orders": {"tags": {}}},
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli,
                [
                    "entity",
                    "tags",
                    "bronze.orders",
                    "--app",
                    str(app_path),
                    "--reveal-secrets",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "WARNING" in result.output
        assert "@secret:kv/orders-table" in result.output

    def test_dataentities_bytag_provenance_from_yaml(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        entity = _make_entity(
            "bronze.orders", tags={"tier": "bronze", "provider.table_catalog": "dev_bronze"}
        )
        registry = _FakeEntityRegistry(
            {"bronze.orders": entity},
            raw_params={"bronze.orders": {"tags": {"tier": "bronze"}}},
        )
        _patch_registry(monkeypatch, DataEntityRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "dataentities-bytag:\n"
                "  tier:\n"
                "    bronze:\n"
                "      tags:\n"
                "        provider.table_catalog: dev_bronze\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                cli, ["entity", "tags", "bronze.orders", "--app", str(app_path), "--json"]
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["provenance"]["provider.table_catalog"] == "dataentities-bytag:"

    def test_env_falls_back_to_kindling_env(self, monkeypatch):
        from kindling.data_entities import DataEntityRegistry

        entity = _make_entity("bronze.orders", tags={"provider_type": "delta"})
        registry = _FakeEntityRegistry({"bronze.orders": entity})
        _patch_registry(monkeypatch, DataEntityRegistry, registry)
        monkeypatch.setenv("KINDLING_ENV", "staging")

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli, ["entity", "tags", "bronze.orders", "--app", str(app_path), "--json"]
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["env"] == "staging"


# ---------------------------------------------------------------------------
# kindling pipeline show
# ---------------------------------------------------------------------------


class TestPipelineShow:
    def test_unknown_pipe_fails(self, monkeypatch):
        from kindling.data_pipes import DataPipesRegistry

        _patch_registry(monkeypatch, DataPipesRegistry, _FakePipeRegistry({}))

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli, ["pipeline", "show", "missing.pipe", "--app", str(app_path)]
            )

        assert result.exit_code != 0
        assert "not registered" in result.output

    def test_shows_structure_by_default(self, monkeypatch):
        from kindling.data_pipes import DataPipesRegistry

        pipe = _make_pipe(
            "bronze.ingest_orders",
            input_entity_ids=["raw.orders"],
            output_entity_id="bronze.orders",
            output_type="delta",
        )
        _patch_registry(
            monkeypatch, DataPipesRegistry, _FakePipeRegistry({"bronze.ingest_orders": pipe})
        )

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli, ["pipeline", "show", "bronze.ingest_orders", "--app", str(app_path)]
            )

        assert result.exit_code == 0, result.output
        assert "raw.orders" in result.output
        assert "bronze.orders" in result.output
        assert "delta" in result.output

    def test_json_structure_output(self, monkeypatch):
        from kindling.data_pipes import DataPipesRegistry

        pipe = _make_pipe(
            "bronze.ingest_orders",
            input_entity_ids=["raw.orders"],
            output_entity_id="bronze.orders",
            output_type="delta",
        )
        _patch_registry(
            monkeypatch, DataPipesRegistry, _FakePipeRegistry({"bronze.ingest_orders": pipe})
        )

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli,
                ["pipeline", "show", "bronze.ingest_orders", "--app", str(app_path), "--json"],
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload == {
            "pipe_id": "bronze.ingest_orders",
            "name": "bronze.ingest_orders",
            "input_entity_ids": ["raw.orders"],
            "output_entity_id": "bronze.orders",
            "output_type": "delta",
        }

    def test_tags_flag_shows_provenance_from_datapipes_bytag(self, monkeypatch):
        from kindling.data_pipes import DataPipesRegistry

        pipe = _make_pipe(
            "bronze.ingest_orders", tags={"tier": "bronze", "retry.max_attempts": "5"}
        )
        registry = _FakePipeRegistry(
            {"bronze.ingest_orders": pipe},
            raw_params={"bronze.ingest_orders": {"tags": {"tier": "bronze"}}},
        )
        _patch_registry(monkeypatch, DataPipesRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "datapipes-bytag:\n"
                "  tier:\n"
                "    bronze:\n"
                "      tags:\n"
                "        retry.max_attempts: '5'\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                cli,
                [
                    "pipeline",
                    "show",
                    "bronze.ingest_orders",
                    "--app",
                    str(app_path),
                    "--tags",
                    "--json",
                ],
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["tags"]["retry.max_attempts"] == "5"
        assert payload["provenance"]["retry.max_attempts"] == "datapipes-bytag:"
        assert payload["provenance"]["tier"] == "literal tags="

    def test_tags_secret_redaction(self, monkeypatch):
        from kindling.data_pipes import DataPipesRegistry

        pipe = _make_pipe("bronze.ingest_orders", tags={"provider.token": "resolved-live-value"})
        registry = _FakePipeRegistry(
            {"bronze.ingest_orders": pipe},
            raw_params={"bronze.ingest_orders": {"tags": {}}},
        )
        _patch_registry(monkeypatch, DataPipesRegistry, registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "datapipes:\n"
                "  bronze.ingest_orders:\n"
                "    tags:\n"
                "      provider.token: '@secret:kv/token'\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                cli,
                [
                    "pipeline",
                    "show",
                    "bronze.ingest_orders",
                    "--app",
                    str(app_path),
                    "--tags",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "<secret: kv/token>" in result.output
        assert "@secret:kv/token" not in result.output
