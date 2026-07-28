"""
Unit tests for kindling.config_patterns (gh#31 Wildcard Config Patterns).

Covers the pattern language (* / ? / ** over dot-segmented ids), tiered
specificity, least→most-specific ordering with declaration-order ties,
copy-on-merge override resolution, the issue's and the proposal's
end-to-end vectors, and quoted wildcard keys loaded through real Dynaconf
exactly as DynaconfConfig.initialize loads config files.
"""

import copy
import logging
import textwrap
from collections.abc import Mapping

import pytest
from dynaconf import Dynaconf
from kindling.config_patterns import ConfigPatternMatcher


def _matches(pattern: str, item_id: str) -> bool:
    matcher = ConfigPatternMatcher({pattern: {"src": pattern}})
    return bool(matcher.get_matching_overrides(item_id))


def _match_order(section, item_id):
    """Return the ``src`` labels of matching overrides in application order."""
    matcher = ConfigPatternMatcher(section)
    return [overrides["src"] for overrides in matcher.get_matching_overrides(item_id)]


class TestPatternTranslation:
    @pytest.mark.parametrize(
        ("pattern", "item_id", "expected"),
        [
            # Exact patterns match only themselves
            ("bronze.ingest_orders", "bronze.ingest_orders", True),
            ("bronze.ingest_orders", "bronze.ingest_orders_v2", False),
            ("bronze.ingest_orders", "bronze.ingest", False),
            ("Bronze.ingest_orders", "bronze.ingest_orders", False),  # case-sensitive
            # * matches one non-empty span within a segment
            ("bronze.*", "bronze.orders", True),
            ("bronze.*", "bronze.customers", True),
            ("bronze.*", "bronze.orders.raw", False),
            ("bronze.*", "silver.orders", False),
            ("bronze.*", "bronze", False),
            # * may be embedded; still never crosses dots and needs 1+ chars
            ("*.ingest_*", "bronze.ingest_orders", True),
            ("*.ingest_*", "ingest_orders", False),
            ("*.ingest_*", "a.b.ingest_x", False),
            ("*.ingest_*", "bronze.ingest_", False),
            ("bronze*", "bronze.orders", False),
            ("bronze*x", "bronzeYx", True),
            # ? matches exactly one non-dot character
            ("bronze.order?", "bronze.orders", True),
            ("bronze.order?", "bronze.order1", True),
            ("bronze.order?", "bronze.order", False),
            ("bronze.order?", "bronze.order12", False),
            ("bronze?orders", "bronze.orders", False),
            # ** matches one or more whole segments
            ("**", "a", True),
            ("**", "a.b", True),
            ("**", "a.b.c", True),
            ("bronze.**", "bronze.x", True),
            ("bronze.**", "bronze.x.y", True),
            ("bronze.**", "bronze", False),
            ("**.ingest", "bronze.ingest", True),
            ("**.ingest", "silver.etl.ingest", True),
            ("**.ingest", "ingest", False),
            # ** never accepts ids with empty segments (review finding)
            ("**", "a..b", False),
            ("**", "a.", False),
            ("**", ".a", False),
            ("bronze.**", "bronze..x", False),
            ("bronze.**", "bronze.x.", False),
            # Regex metacharacters in ids are literal
            ("raw.a+b", "raw.a+b", True),
            ("raw.a+b", "raw.aab", False),
            ("etl-v1.load", "etl-v1.load", True),
        ],
    )
    def test_match(self, pattern, item_id, expected):
        assert _matches(pattern, item_id) is expected

    @pytest.mark.parametrize(
        "malformed",
        ["ingest_**", "**x", "bronze.**suffix", "", "bronze..orders", ".bronze", "bronze."],
    )
    def test_malformed_patterns_warn_and_are_skipped(self, caplog, malformed):
        section = {malformed: {"src": "bad"}, "bronze.*": {"src": "good"}}
        with caplog.at_level(logging.WARNING, logger="kindling.config"):
            matcher = ConfigPatternMatcher(section)
        assert any("skipping" in record.message.lower() for record in caplog.records)
        # The malformed pattern contributes nothing; valid siblings still work.
        assert matcher.get_matching_overrides("bronze.x") == [{"src": "good"}]
        assert matcher.get_matching_overrides("ingest_x") == []

    def test_keys_are_coerced_via_str(self):
        matcher = ConfigPatternMatcher({123: {"src": "numeric"}})
        assert matcher.get_matching_overrides("123") == [{"src": "numeric"}]


class TestSpecificity:
    def test_tier_constants_keep_proposal_values(self):
        assert ConfigPatternMatcher.EXACT_MATCH == 1000
        assert ConfigPatternMatcher.SINGLE_WILDCARD == 100
        assert ConfigPatternMatcher.MULTI_WILDCARD == 10

    @pytest.mark.parametrize(
        ("pattern", "tier"),
        [
            ("bronze.ingest_orders", 1000),
            ("etl-v1.load+x", 1000),
            ("bronze.*", 100),
            ("*.ingest_*", 100),
            ("bronze.order?", 100),
            ("**", 10),
            ("bronze.**", 10),
            ("**.ingest", 10),
            ("*.x.**", 10),
        ],
    )
    def test_tiers(self, pattern, tier):
        assert ConfigPatternMatcher.specificity(pattern) == tier

    def test_single_wildcard_patterns_tie(self):
        # Resolved Decision #3: bronze.* and *.orders are EQUAL specificity.
        assert ConfigPatternMatcher.specificity("bronze.*") == ConfigPatternMatcher.specificity(
            "*.orders"
        )


class TestOrdering:
    def test_least_to_most_specific(self):
        section = {
            "bronze.ingest_orders": {"src": "exact"},
            "**": {"src": "multi"},
            "bronze.*": {"src": "single"},
        }
        assert _match_order(section, "bronze.ingest_orders") == [
            "multi",
            "single",
            "exact",
        ]

    def test_declaration_order_preserved_within_tier(self):
        forward = {"bronze.*": {"src": "first"}, "*.ingest_*": {"src": "second"}}
        reversed_section = {"*.ingest_*": {"src": "first"}, "bronze.*": {"src": "second"}}
        assert _match_order(forward, "bronze.ingest_x") == ["first", "second"]
        assert _match_order(reversed_section, "bronze.ingest_x") == ["first", "second"]

    def test_later_declared_wins_tied_scalar_conflicts(self):
        forward = {"bronze.*": {"retry_count": 3}, "*.ingest_*": {"retry_count": 5}}
        reversed_section = {
            "*.ingest_*": {"retry_count": 5},
            "bronze.*": {"retry_count": 3},
        }
        assert (
            ConfigPatternMatcher(forward).resolve_overrides("bronze.ingest_x", {})["retry_count"]
            == 5
        )
        assert (
            ConfigPatternMatcher(reversed_section).resolve_overrides("bronze.ingest_x", {})[
                "retry_count"
            ]
            == 3
        )


class TestMergeSemantics:
    def test_tags_accumulate_across_all_matching_tiers(self):
        section = {
            "**": {"tags": {"framework": "kindling"}},
            "bronze.*": {"tags": {"layer": "bronze"}},
            "bronze.orders": {"tags": {"priority": "critical"}},
        }
        result = ConfigPatternMatcher(section).resolve_overrides(
            "bronze.orders", {"tags": {"domain": "sales"}}
        )
        assert result["tags"] == {
            "domain": "sales",
            "framework": "kindling",
            "layer": "bronze",
            "priority": "critical",
        }

    def test_scalar_most_specific_wins(self):
        section = {
            "bronze.*": {"timeout_seconds": 300},
            "bronze.orders": {"timeout_seconds": 600},
        }
        result = ConfigPatternMatcher(section).resolve_overrides("bronze.orders", {})
        assert result["timeout_seconds"] == 600

    def test_nested_dicts_deep_merge_recursively(self):
        base = {"spark_config": {"spark": {"sql": {"shuffle.partitions": "200"}}}}
        section = {
            "bronze.*": {"spark_config": {"spark": {"sql": {"ansi.enabled": "true"}}}},
            "bronze.orders": {"spark_config": {"spark": {"sql": {"shuffle.partitions": "8"}}}},
        }
        result = ConfigPatternMatcher(section).resolve_overrides("bronze.orders", base)
        assert result["spark_config"] == {
            "spark": {"sql": {"shuffle.partitions": "8", "ansi.enabled": "true"}}
        }

    def test_lists_replace(self):
        section = {"bronze.*": {"partition_columns": ["c"]}}
        result = ConfigPatternMatcher(section).resolve_overrides(
            "bronze.orders", {"partition_columns": ["a", "b"]}
        )
        assert result["partition_columns"] == ["c"]

    def test_base_is_never_mutated(self):
        base = {
            "tags": {"domain": "sales"},
            "retry": {"attempts": 1},
            "partition_columns": ["a"],
        }
        snapshot = copy.deepcopy(base)
        section = {
            "**": {"tags": {"framework": "kindling"}},
            "bronze.*": {
                "tags": {"layer": "bronze"},
                "retry": {"attempts": 3},
                "partition_columns": ["b"],
            },
        }
        ConfigPatternMatcher(section).resolve_overrides("bronze.orders", base)
        assert base == snapshot

    def test_result_shares_no_nested_containers_with_base(self):
        # Review finding: dict(base) was a shallow copy — nested containers
        # untouched by any override leaked through and mutating the result
        # mutated base. The result must be a full plain-dict deep copy.
        base = {"tags": {"domain": "sales"}, "cols": ["a"]}
        snapshot = copy.deepcopy(base)
        result = ConfigPatternMatcher({"silver.*": {"x": 1}}).resolve_overrides(
            "bronze.orders", base  # no pattern matches; nested keys untouched
        )
        result["tags"]["injected"] = "x"
        result["cols"].append("b")
        assert base == snapshot

    def test_result_converts_custom_mappings_to_plain_dicts(self):
        class Box(dict):
            pass

        base = {"tags": Box({"domain": "sales"})}
        result = ConfigPatternMatcher(None).resolve_overrides("any.id", base)
        assert type(result["tags"]) is dict
        assert result["tags"] == {"domain": "sales"}

    def test_results_are_isolated_from_matcher_state(self):
        section = {"bronze.*": {"tags": {"layer": "bronze"}, "cols": ["a"]}}
        matcher = ConfigPatternMatcher(section)
        first = matcher.resolve_overrides("bronze.orders", {})
        first["tags"]["injected"] = "x"
        first["cols"].append("b")
        assert matcher.resolve_overrides("bronze.orders", {}) == {
            "tags": {"layer": "bronze"},
            "cols": ["a"],
        }

    def test_non_mapping_override_value_warns_and_is_skipped(self, caplog):
        with caplog.at_level(logging.WARNING, logger="kindling.config"):
            matcher = ConfigPatternMatcher(
                {"bronze.*": "oops", "bronze.orders": {"tags": {"a": "1"}}}
            )
        assert any("not a mapping" in record.message for record in caplog.records)
        assert matcher.resolve_overrides("bronze.orders", {}) == {"tags": {"a": "1"}}

    @pytest.mark.parametrize("section", [None, {}])
    def test_none_or_empty_section_returns_base_copy(self, section):
        base = {"tags": {"a": "1"}}
        result = ConfigPatternMatcher(section).resolve_overrides("any.id", base)
        assert result == base
        assert result is not base

    def test_underscore_keys_pass_through_untouched(self):
        # Interpreting _enabled/_remove_tags/_remove_all_tags is tag
        # management's job (upsert model) — the matcher just merges them.
        section = {
            "**": {"tags": {"debug": "true"}},
            "bronze.*": {
                "_remove_tags": ["debug"],
                "_remove_all_tags": True,
                "_enabled": False,
            },
        }
        result = ConfigPatternMatcher(section).resolve_overrides("bronze.orders", {})
        assert result["_remove_tags"] == ["debug"]
        assert result["_remove_all_tags"] is True
        assert result["_enabled"] is False
        assert result["tags"] == {"debug": "true"}


class TestEndToEndVectors:
    def test_issue_31_four_pattern_example(self):
        section = {
            "**": {"tags": {"framework": "kindling"}},
            "bronze.*": {"tags": {"layer": "bronze"}},
            "*.ingest_*": {"tags": {"type": "ingestion"}},
            "bronze.ingest_orders": {"tags": {"priority": "critical"}},
        }
        result = ConfigPatternMatcher(section).resolve_overrides("bronze.ingest_orders", {})
        assert result == {
            "tags": {
                "framework": "kindling",
                "layer": "bronze",
                "type": "ingestion",
                "priority": "critical",
            }
        }

    # The proposal's resolution-trace config with settings + env_prod already
    # collapsed per pattern key, as Dynaconf MERGE_ENABLED delivers it.
    TRACE_SECTION = {
        "**": {
            "tags": {"framework": "kindling", "managed": "true"},
            "_remove_tags": ["debug", "test"],
        },
        "bronze.*": {
            "tags": {"layer": "bronze", "sla": "4h"},
            "timeout_seconds": 300,
            "retry_count": 3,
        },
        "*.ingest_*": {"tags": {"type": "ingestion"}, "retry_count": 5},
        "bronze.ingest_orders": {
            "tags": {
                "priority": "critical",
                "owner": "team-a",
                "environment": "production",
                "alert_channel": "#orders-alerts",
            },
            "timeout_seconds": 600,
            "retry_count": 10,
        },
    }

    def test_proposal_resolution_trace(self):
        base = {"name": "Ingest Orders", "tags": {"domain": "sales"}}
        result = ConfigPatternMatcher(self.TRACE_SECTION).resolve_overrides(
            "bronze.ingest_orders", base
        )
        assert result["name"] == "Ingest Orders"
        assert result["timeout_seconds"] == 600
        assert result["retry_count"] == 10
        assert result["tags"] == {
            "domain": "sales",
            "framework": "kindling",
            "managed": "true",
            "layer": "bronze",
            "sla": "4h",
            "type": "ingestion",
            "priority": "critical",
            "owner": "team-a",
            "environment": "production",
            "alert_channel": "#orders-alerts",
        }
        # Underscore keys survive as data for tag management to interpret.
        assert result["_remove_tags"] == ["debug", "test"]

    def test_proposal_trace_tie_between_single_wildcards(self):
        # retry_count 5 from *.ingest_* must survive bronze.*'s 3 — the two
        # patterns tie on specificity and *.ingest_* is declared later.
        result = ConfigPatternMatcher(self.TRACE_SECTION).resolve_overrides(
            "bronze.ingest_customers", {}
        )
        assert result["retry_count"] == 5
        assert result["timeout_seconds"] == 300


class TestRealPipeConfigIntegration:
    """Quoted wildcard keys survive YAML→Dynaconf and DynaBox sections work as-is."""

    SETTINGS_YAML = textwrap.dedent("""\
        datapipes:
          "**":
            tags:
              framework: "kindling"
              managed: "true"
          "bronze.*":
            tags:
              layer: "bronze"
              sla: "4h"
            timeout_seconds: 300
            retry_count: 3
          "*.ingest_*":
            tags:
              type: "ingestion"
            retry_count: 5
          "bronze.ingest_orders":
            tags:
              priority: "critical"
              owner: "team-a"
        """)

    ENV_PROD_YAML = textwrap.dedent("""\
        datapipes:
          "**":
            _remove_tags:
              - debug
              - test
          "bronze.ingest_orders":
            tags:
              environment: "production"
              alert_channel: "#orders-alerts"
            timeout_seconds: 600
            retry_count: 10
        """)

    def test_dynabox_section_resolves_like_plain_dict(self, tmp_path):
        settings_path = tmp_path / "settings.yaml"
        env_prod_path = tmp_path / "env_prod.yaml"
        settings_path.write_text(self.SETTINGS_YAML, encoding="utf-8")
        env_prod_path.write_text(self.ENV_PROD_YAML, encoding="utf-8")

        # Load exactly as DynaconfConfig.initialize does (spark_config.py).
        settings = Dynaconf(
            settings_files=[str(settings_path), str(env_prod_path)],
            environments=False,
            MERGE_ENABLED_FOR_DYNACONF=True,
            envvar_prefix="KINDLING",
        )
        section = settings.get("datapipes")

        # The section is fed to the matcher as the DynaBox Dynaconf returns,
        # not converted to a plain dict first.
        assert isinstance(section, Mapping)
        assert type(section) is not dict

        matcher = ConfigPatternMatcher(section)
        base = {"name": "Ingest Orders", "tags": {"domain": "sales"}}
        result = matcher.resolve_overrides("bronze.ingest_orders", base)

        assert result["timeout_seconds"] == 600
        assert result["retry_count"] == 10
        assert result["tags"] == {
            "domain": "sales",
            "framework": "kindling",
            "managed": "true",
            "layer": "bronze",
            "sla": "4h",
            "type": "ingestion",
            "priority": "critical",
            "owner": "team-a",
            "environment": "production",
            "alert_channel": "#orders-alerts",
        }
        assert result["_remove_tags"] == ["debug", "test"]

        # An id the exact/env overrides don't target resolves from the
        # wildcard tiers of the same DynaBox section.
        other = matcher.resolve_overrides("bronze.ingest_customers", {"tags": {}})
        assert other["retry_count"] == 5
        assert other["timeout_seconds"] == 300
        assert other["tags"]["layer"] == "bronze"
