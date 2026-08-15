"""Matching engines for per-item configuration override sections.

Config sections such as ``datapipes:`` / ``dataentities:`` (and existing
per-item maps like ``kindling.execution.pipes``) key overrides by pipe or
entity id. :class:`ConfigPatternMatcher` provides the pattern engine that
lets those keys be glob patterns over dot-segmented ids, so one entry can
target a family of items (``bronze.*``) instead of every id individually.
:class:`TagRuleMatcher` is the general-purpose counterpart for
``dataentities_by_tag:``-style sections: it matches by one of the item's own
already-declared tag VALUES instead of its id, and (like
``ConfigPatternMatcher``) can set any overridable field, not just a specific
namespace. Both are pure stdlib — no framework imports, no Spark, no
Dynaconf dependency. Any ``Mapping`` (including a Dynaconf ``DynaBox``)
works as the config section; ``ConfigPatternMatcher`` is indexed by literal
pattern string, never dotted-key traversal, because ids routinely contain
dots (same rationale as ``kindling.execution.pipes``).

Pattern language (full-string anchored ``^...$``, case-sensitive, over
dot-segmented ids):

- ``*`` — one non-empty run of characters within a segment; may be
  embedded (``ingest_*``). Never crosses a ``.``, so ``bronze.*`` matches
  ``bronze.orders`` but not ``bronze.orders.raw``.
- ``?`` — exactly one non-dot character.
- ``**`` — one or more whole segments; only special as a complete segment
  (``bronze.**`` matches ``bronze.x`` and ``bronze.x.y`` but not bare
  ``bronze``; bare ``**`` matches every id). A ``**`` embedded mid-segment
  (``ingest_**``), an empty pattern, or an empty segment is malformed: a
  warning is logged and the pattern is skipped.
- Every other character is literal (ids containing ``_``, ``-``, ``+``
  need no escaping).

Specificity is tiered — :data:`ConfigPatternMatcher.EXACT_MATCH` (no
wildcard characters) beats :data:`ConfigPatternMatcher.SINGLE_WILDCARD`
(only ``*``/``?``), which beats :data:`ConfigPatternMatcher.MULTI_WILDCARD`
(contains ``**``). Overrides resolve least→most specific; within a tier the
mapping's declaration order is kept and the later-declared pattern is
applied later, so last-in wins ties (Resolved Decision #3 of
``docs/proposals/package_config_architecture.md``).

Merge semantics (:meth:`ConfigPatternMatcher.resolve_overrides`): mapping
values deep-merge recursively (``tags``, ``delta_properties``,
``spark_config``, ``retry`` all behave uniformly); scalars and lists
replace. The input ``base`` is never mutated — nested containers are
copied on write. Underscore keys (``_enabled``, ``_remove_tags``,
``_remove_all_tags``) get no special handling here: they merge like any
other key (upsert model) and are interpreted downstream by tag management.

Layering: the matcher sees the ALREADY-MERGED section — Dynaconf
``MERGE_ENABLED_FOR_DYNACONF`` collapses the settings → platform →
workspace → environment overlays per pattern key before resolution, so
same-pattern overrides across layers behave as expected and cross-pattern
conflicts resolve by tier then declaration order. Caveat: a pattern
re-declared in a later layer keeps its first-seen position in the merged
mapping (dict-merge semantics), so "last-in wins" ties follow the merged
mapping order, not file order across layers — keep tie-sensitive override
patterns declared later within the same mapping.
"""

import logging
import re
from collections.abc import Mapping
from typing import Any, Dict, List, NamedTuple, Optional

_LOGGER = logging.getLogger("kindling.config")


class _CompiledPattern(NamedTuple):
    pattern: str
    regex: re.Pattern
    tier: int
    index: int
    overrides: Dict[str, Any]


def _clone(value: Any) -> Any:
    """Deep-copy mappings and lists into plain dicts/lists; scalars pass through."""
    if isinstance(value, Mapping):
        return {key: _clone(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clone(item) for item in value]
    return value


def _merge(base: Mapping, override: Mapping) -> Dict[str, Any]:
    """Merge ``override`` into a copy of ``base``; neither input is mutated."""
    result: Dict[str, Any] = dict(base)
    for key, value in override.items():
        existing = result.get(key)
        if isinstance(value, Mapping) and isinstance(existing, Mapping):
            result[key] = _merge(existing, value)
        else:
            result[key] = _clone(value)
    return result


class ConfigPatternMatcher:
    """Matches glob config patterns against pipe/entity ids with precedence."""

    # Specificity tiers (ranks, not per-segment sums): exact > single > multi.
    EXACT_MATCH = 1000
    SINGLE_WILDCARD = 100
    MULTI_WILDCARD = 10

    def __init__(self, config_section: Optional[Mapping] = None):
        """Compile and order a ``{pattern: overrides}`` section once.

        Args:
            config_section: Mapping keyed by literal pattern string (plain
                dict or Dynaconf ``DynaBox``). ``None``/empty means no
                patterns. Keys are coerced via ``str()``; non-mapping
                override values and malformed patterns log a warning and
                are skipped.
        """
        self._patterns = self._compile(config_section)

    @staticmethod
    def specificity(pattern: str) -> int:
        """Return the specificity tier for ``pattern``.

        ``EXACT_MATCH`` when it has no wildcard characters,
        ``SINGLE_WILDCARD`` when it uses only ``*``/``?``, and
        ``MULTI_WILDCARD`` when it contains ``**``.
        """
        if "**" in pattern:
            return ConfigPatternMatcher.MULTI_WILDCARD
        if "*" in pattern or "?" in pattern:
            return ConfigPatternMatcher.SINGLE_WILDCARD
        return ConfigPatternMatcher.EXACT_MATCH

    def get_matching_overrides(self, item_id: str) -> List[Dict[str, Any]]:
        """Return all override dicts matching ``item_id``, least→most specific.

        Declaration order is stable within a tier. The returned dicts are
        copies; apply them in list order to get correct precedence.
        """
        return [
            _clone(compiled.overrides)
            for compiled in self._patterns
            if compiled.regex.match(item_id)
        ]

    def resolve_overrides(self, item_id: str, base: Mapping) -> Dict[str, Any]:
        """Return a plain-dict deep copy of ``base`` with every matching
        override merged in order.

        Mapping values deep-merge recursively; scalars and lists replace.
        ``base`` is never mutated — the result shares no nested containers
        with it (non-dict Mappings such as Dynaconf boxes are converted to
        plain dicts). Underscore keys (``_enabled``, ``_remove_tags``, ...)
        merge like any other key.
        """
        result: Dict[str, Any] = _clone(base)
        for compiled in self._patterns:
            if compiled.regex.match(item_id):
                result = _merge(result, compiled.overrides)
        return result

    def _compile(self, config_section: Optional[Mapping]) -> List[_CompiledPattern]:
        compiled: List[_CompiledPattern] = []
        if not config_section:
            return compiled
        for index, (key, overrides) in enumerate(config_section.items()):
            pattern = str(key)
            if not isinstance(overrides, Mapping):
                _LOGGER.warning(
                    "Config pattern %r: override value %r is not a mapping — skipping",
                    pattern,
                    overrides,
                )
                continue
            regex = self._pattern_to_regex(pattern)
            if regex is None:
                continue
            compiled.append(
                _CompiledPattern(
                    pattern, regex, self.specificity(pattern), index, _clone(overrides)
                )
            )
        # Least specific first; within a tier keep declaration order so the
        # later-declared pattern is applied later (last-in wins ties).
        compiled.sort(key=lambda entry: (entry.tier, entry.index))
        return compiled

    @staticmethod
    def _pattern_to_regex(pattern: str) -> Optional[re.Pattern]:
        """Translate a glob pattern to an anchored regex; ``None`` if malformed."""
        if not pattern:
            _LOGGER.warning("Config pattern is empty — skipping")
            return None
        segment_sources: List[str] = []
        for segment in pattern.split("."):
            if not segment:
                _LOGGER.warning("Config pattern %r has an empty segment — skipping", pattern)
                return None
            if segment == "**":
                # One or more whole (non-empty, dot-free) segments — ids with
                # empty segments like "a..b" or "a." must not match.
                segment_sources.append(r"[^.]+(?:\.[^.]+)*")
                continue
            if "**" in segment:
                _LOGGER.warning(
                    "Config pattern %r: '**' must stand alone as a segment — skipping",
                    pattern,
                )
                return None
            source = ""
            for char in segment:
                if char == "*":
                    source += "[^.]+"
                elif char == "?":
                    source += "[^.]"
                else:
                    source += re.escape(char)
            segment_sources.append(source)
        return re.compile("^" + r"\.".join(segment_sources) + "$")


class TagRuleMatcher:
    """General-purpose counterpart to :class:`ConfigPatternMatcher`, matching
    by an entity's own already-declared tag VALUE instead of its id.

    Config shape: ``{tag_key: {tag_value: overrides}}``, e.g.::

        dataentities_by_tag:
          tier:
            bronze:
              tags:
                provider.table_catalog: dev_bronze
            gold:
              tags:
                provider.table_catalog: dev_gold
                schema.drift: fail

    Every entity whose ``tier`` tag is ``bronze`` picks up
    ``provider.table_catalog: dev_bronze`` (merged into its existing tags);
    every ``tier: gold`` entity additionally gets a stricter drift policy.
    ``overrides`` can set any overridable field, not just ``tags`` — same
    deep-merge semantics as :class:`ConfigPatternMatcher`
    (:func:`resolve_overrides`): mappings deep-merge recursively, scalars and
    lists replace, ``base`` is never mutated.

    Reach for this when placement/config follows a semantic tag rather than
    an entityid naming convention; reach for :class:`ConfigPatternMatcher`
    (``dataentities:``) when it follows an entityid convention. The two
    compose: apply this matcher first (broad, tag-driven defaults), then
    :class:`ConfigPatternMatcher` on top (specific, id-driven overrides) so a
    targeted ``dataentities:`` entry can still override a broader tag-based
    default for one entity.

    Multiple tag keys can match the same entity; declaration order (top to
    bottom in the config section) determines application order, so a
    later-declared tag key's overrides win ties on the same field — same
    "declaration order, last-in wins" rule as :class:`ConfigPatternMatcher`.
    An entity can only have one value for a given tag key, so there's no tie
    to break *within* a single tag key.
    """

    def __init__(self, config_section: Optional[Mapping] = None):
        """Compile and order a ``{tag_key: {tag_value: overrides}}`` section once.

        Args:
            config_section: Mapping keyed by tag key, each value itself a
                mapping of tag value -> overrides. ``None``/empty means no
                rules. Non-mapping entries at either level log a warning and
                are skipped.
        """
        self._rules = self._compile(config_section)

    def get_matching_overrides(self, entity_tags: Mapping) -> List[Dict[str, Any]]:
        """Return all override dicts whose tag key/value matches ``entity_tags``,
        in declaration order. The returned dicts are copies; apply them in
        list order to get correct precedence.
        """
        matches = []
        for tag_key, value_map, _index in self._rules:
            tag_value = entity_tags.get(tag_key)
            if tag_value is None:
                continue
            overrides = value_map.get(str(tag_value))
            if overrides is not None:
                matches.append(_clone(overrides))
        return matches

    def resolve_overrides(self, entity_tags: Mapping, base: Mapping) -> Dict[str, Any]:
        """Return a plain-dict deep copy of ``base`` with every matching
        tag rule's overrides merged in, in declaration order.
        """
        result: Dict[str, Any] = _clone(base)
        for overrides in self.get_matching_overrides(entity_tags):
            result = _merge(result, overrides)
        return result

    def _compile(self, config_section: Optional[Mapping]) -> List[tuple]:
        compiled: List[tuple] = []
        if not config_section:
            return compiled
        for index, (tag_key, value_map) in enumerate(config_section.items()):
            if not isinstance(value_map, Mapping):
                _LOGGER.warning(
                    "Tag rule %r: value %r is not a mapping of tag-value -> overrides — skipping",
                    tag_key,
                    value_map,
                )
                continue
            cleaned_map: Dict[str, Any] = {}
            for tag_value, overrides in value_map.items():
                if not isinstance(overrides, Mapping):
                    _LOGGER.warning(
                        "Tag rule %r=%r: override value %r is not a mapping — skipping",
                        tag_key,
                        tag_value,
                        overrides,
                    )
                    continue
                cleaned_map[str(tag_value)] = overrides
            if cleaned_map:
                compiled.append((str(tag_key), cleaned_map, index))
        return compiled
