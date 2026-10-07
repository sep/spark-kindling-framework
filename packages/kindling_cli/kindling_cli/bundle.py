"""Assemble a Databricks bundle from a Kindling project (design-time only).

Implements ``kindling bundle build`` -- see
``docs/proposals/databricks_bundle_deployment.md``. Everything here is
Spark-free and never imports app code. Inputs are the project's settings
YAML overlays plus typed deployment values from CLI options or
``KINDLING_BUNDLE_*`` environment variables; output is a disposable bundle
directory that the Databricks CLI validates, deploys and runs.

Configuration travels inline. The generator applies Kindling's settings
convention (shared ``config/`` overlays, then the app's own ``settings*.yaml``)
to find the files, merges them at build time in the runtime's order
(base -> platform -> workspace -> environment -> app), and writes the result
into the pipeline resource as the ``kindling.lakeflow.settings_json``
configuration value. No file list is written anywhere and the deployed
pipeline reads no settings files from the workspace or a volume; the resource
file is the complete description of what the pipeline runs with.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import jinja2
import yaml
from kindling_cli.scaffold import _kindling_version

GENERATOR_NAME = "kindling bundle build"
PLATFORM_OVERLAY = "databricks"
DEFAULT_OUTPUT_SUBDIR = PurePosixPath("dist/bundles/databricks")
DEFAULT_APP_DIR_CANDIDATES = ("data-apps", "apps")
DEFAULT_CONFIG_DIR = "config"
DEFAULT_DEPENDENCIES = ("spark-kindling-ext-databricks",)
SOURCE_FILE = PurePosixPath("src/kindling_lakeflow.py")
RESOURCES_DIR = PurePosixPath("resources")
WHEELS_DIR = PurePosixPath("wheels")
MANIFEST_FILE = "manifest.json"
ENV_PREFIX = "KINDLING_BUNDLE_"

#: Pipeline-configuration keys the Lakeflow selector reads by point lookup on
#: restricted runtimes without being named in ``kindling.lakeflow.config_keys``.
#: Mirrors the selector's default lookup list (a unit test keeps them in sync);
#: the CLI deliberately does not import the runtime extension.
SELECTOR_DEFAULT_LOOKUP_KEYS = frozenset(
    {
        "kindling.data_app",
        "kindling.lakeflow.allowed_apps",
        "kindling.lakeflow.config_keys",
        "kindling.lakeflow.settings_json",
        "kindling.lakeflow.pipes",
    }
)

#: Heuristic only: Databricks documents no limit for pipeline configuration
#: values, so the generator reports the size and warns past this point.
INLINE_SETTINGS_WARN_BYTES = 32 * 1024

_PIPELINE_PERMISSION_LEVELS = ("CAN_VIEW", "CAN_RUN", "CAN_MANAGE", "IS_OWNER")
_PERMISSION_PRINCIPAL_KEYS = ("user_name", "service_principal_name", "group_name")
_APP_OPTION_KEYS = ("catalog", "schema", "continuous", "pipes", "pipelines")
_PIPELINE_OPTION_KEYS = ("catalog", "schema", "continuous", "pipes")
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_APP_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_DYNACONF_DIRECTIVES = ("@merge", "@insert", "@del", "@reset")


class BundleError(Exception):
    """Base error for bundle assembly; the CLI reports the message verbatim."""


class BundleInputError(BundleError):
    """A deployment input is missing, malformed, or inconsistent."""


class BundleProjectError(BundleError):
    """The project layout or its settings files cannot be assembled."""


# --------------------------------------------------------------------------- #
# Deployment inputs
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PipelineSpec:
    """One generated pipeline resource: an app plus its deployment choices."""

    app: str
    suffix: Optional[str]
    catalog: str
    schema: str
    continuous: bool
    pipes: Tuple[str, ...]

    @property
    def key(self) -> str:
        """Stable bundle resource key (identity of the deployed pipeline)."""
        parts = [self.app] + ([self.suffix] if self.suffix else [])
        return "_".join(_resource_key(part) for part in parts)

    @property
    def name(self) -> str:
        return self.app if not self.suffix else f"{self.app}-{self.suffix}"

    @property
    def resource_file(self) -> PurePosixPath:
        return RESOURCES_DIR / f"{self.key}.pipeline.yml"


@dataclass(frozen=True)
class BundleInputs:
    """Validated, non-secret deployment inputs for one bundle build."""

    name: str
    target: str
    apps: Tuple[str, ...]
    workspace_host: str
    workspace_root: str
    runtime_env: str
    workspace_id: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None
    continuous: bool = False
    run_as_service_principal: Optional[str] = None
    app_options: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    permissions: Tuple[Dict[str, str], ...] = ()
    dependencies: Tuple[str, ...] = DEFAULT_DEPENDENCIES
    dependencies_defaulted: bool = False
    wheels: Tuple[Path, ...] = ()

    def pipelines(self) -> List[PipelineSpec]:
        specs: List[PipelineSpec] = []
        for app in self.apps:
            options = self.app_options.get(app, {})
            app_defaults = {
                "catalog": options.get("catalog", self.catalog),
                "schema": options.get("schema", self.schema),
                "continuous": options.get("continuous", self.continuous),
                "pipes": tuple(options.get("pipes", ())),
            }
            nested = options.get("pipelines")
            if nested:
                for suffix, pipeline_options in nested.items():
                    merged = dict(app_defaults)
                    merged.update(
                        {
                            key: pipeline_options[key]
                            for key in _PIPELINE_OPTION_KEYS
                            if key in pipeline_options
                        }
                    )
                    specs.append(self._pipeline_spec(app, suffix, merged))
            else:
                specs.append(self._pipeline_spec(app, None, app_defaults))
        keys = [spec.key for spec in specs]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            raise BundleInputError(
                "Pipeline resource keys must be unique; duplicates: " + ", ".join(duplicates)
            )
        return specs

    @staticmethod
    def _pipeline_spec(app: str, suffix: Optional[str], options: Mapping[str, Any]) -> PipelineSpec:
        label = app if suffix is None else f"{app}/{suffix}"
        catalog = options.get("catalog")
        schema = options.get("schema")
        if not catalog:
            raise BundleInputError(
                f"Pipeline '{label}' has no catalog. Pass --catalog / "
                f"{ENV_PREFIX}CATALOG or set it in --app-options-json."
            )
        if not schema:
            raise BundleInputError(
                f"Pipeline '{label}' has no schema. Pass --schema / "
                f"{ENV_PREFIX}SCHEMA or set it in --app-options-json."
            )
        return PipelineSpec(
            app=app,
            suffix=suffix,
            catalog=str(catalog),
            schema=str(schema),
            continuous=bool(options.get("continuous", False)),
            pipes=tuple(options.get("pipes", ())),
        )


def _resource_key(text: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_]", "_", text.strip())
    if not key or key[0].isdigit():
        key = f"_{key}"
    return key


def _env_value(environ: Mapping[str, str], suffix: str) -> Optional[str]:
    value = environ.get(f"{ENV_PREFIX}{suffix}")
    if value is None:
        return None
    value = value.strip()
    return value or None


def _scalar_input(
    cli_value: Optional[str],
    environ: Mapping[str, str],
    env_suffix: str,
    default: Optional[str] = None,
) -> Optional[str]:
    if cli_value is not None and str(cli_value).strip():
        return str(cli_value).strip()
    env_value = _env_value(environ, env_suffix)
    if env_value is not None:
        return env_value
    return default


def _bool_input(
    cli_value: Optional[bool], environ: Mapping[str, str], env_suffix: str, default: bool
) -> bool:
    if cli_value is not None:
        return bool(cli_value)
    raw = _env_value(environ, env_suffix)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise BundleInputError(
        f"{ENV_PREFIX}{env_suffix} must be exactly 'true' or 'false' (got {raw!r})."
    )


def _json_input(
    cli_value: Optional[str], environ: Mapping[str, str], env_suffix: str, option: str
) -> Any:
    raw = cli_value if cli_value is not None and str(cli_value).strip() else None
    source = option
    if raw is None:
        raw = _env_value(environ, env_suffix)
        source = f"{ENV_PREFIX}{env_suffix}"
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise BundleInputError(f"{source} is not valid JSON: {exc}") from exc


def _string_list_input(
    cli_values: Sequence[str], environ: Mapping[str, str], env_suffix: str, option: str
) -> Optional[List[str]]:
    values = [str(value).strip() for value in cli_values if str(value).strip()]
    if values:
        return values
    parsed = _json_input(None, environ, env_suffix, option)
    if parsed is None:
        return None
    if not isinstance(parsed, list) or not all(
        isinstance(item, str) and item.strip() for item in parsed
    ):
        raise BundleInputError(
            f"{ENV_PREFIX}{env_suffix} must be a JSON array of non-empty strings."
        )
    return [item.strip() for item in parsed]


def _require(value: Optional[str], option: str, env_suffix: str) -> str:
    if not value:
        raise BundleInputError(
            f"Missing deployment input: pass {option} or set {ENV_PREFIX}{env_suffix}."
        )
    return value


def _validate_app_options(raw: Any, apps: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise BundleInputError("--app-options-json must be a JSON object keyed by app name.")
    unknown = sorted(set(raw) - set(apps))
    if unknown:
        raise BundleInputError(
            "--app-options-json names apps that are not in the managed app set: "
            + ", ".join(unknown)
        )
    validated: Dict[str, Dict[str, Any]] = {}
    for app, options in raw.items():
        validated[app] = _validate_pipeline_options(options, f"app '{app}'", _APP_OPTION_KEYS)
        nested = validated[app].get("pipelines")
        if nested is not None:
            if not isinstance(nested, dict) or not nested:
                raise BundleInputError(
                    f"--app-options-json: 'pipelines' for app '{app}' must be a non-empty "
                    "JSON object keyed by pipeline suffix."
                )
            validated[app]["pipelines"] = {
                str(suffix): _validate_pipeline_options(
                    options_for_suffix, f"pipeline '{app}/{suffix}'", _PIPELINE_OPTION_KEYS
                )
                for suffix, options_for_suffix in nested.items()
            }
            for suffix in validated[app]["pipelines"]:
                if not _APP_NAME_PATTERN.match(suffix):
                    raise BundleInputError(
                        f"--app-options-json: pipeline suffix {suffix!r} for app '{app}' must "
                        "match [A-Za-z0-9][A-Za-z0-9_-]*."
                    )
    return validated


def _validate_pipeline_options(
    options: Any, label: str, allowed_keys: Sequence[str]
) -> Dict[str, Any]:
    if not isinstance(options, dict):
        raise BundleInputError(f"--app-options-json: options for {label} must be a JSON object.")
    unknown = sorted(set(options) - set(allowed_keys))
    if unknown:
        raise BundleInputError(
            f"--app-options-json: unsupported keys for {label}: {', '.join(unknown)} "
            f"(supported: {', '.join(allowed_keys)})."
        )
    validated = dict(options)
    for key in ("catalog", "schema"):
        if key in validated and (not isinstance(validated[key], str) or not validated[key].strip()):
            raise BundleInputError(
                f"--app-options-json: '{key}' for {label} must be a non-empty string."
            )
    if "continuous" in validated and not isinstance(validated["continuous"], bool):
        raise BundleInputError(
            f"--app-options-json: 'continuous' for {label} must be a JSON boolean."
        )
    if "pipes" in validated:
        pipes = validated["pipes"]
        if not isinstance(pipes, list) or not all(
            isinstance(pipe, str) and pipe.strip() for pipe in pipes
        ):
            raise BundleInputError(
                f"--app-options-json: 'pipes' for {label} must be a JSON array of pipe ids."
            )
        validated["pipes"] = [pipe.strip() for pipe in pipes]
    return validated


def _validate_permissions(raw: Any) -> Tuple[Dict[str, str], ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise BundleInputError("--permissions-json must be a JSON array of permission entries.")
    validated: List[Dict[str, str]] = []
    for index, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise BundleInputError(f"--permissions-json entry {index} must be a JSON object.")
        level = entry.get("level")
        if level not in _PIPELINE_PERMISSION_LEVELS:
            raise BundleInputError(
                f"--permissions-json entry {index}: 'level' must be one of "
                f"{', '.join(_PIPELINE_PERMISSION_LEVELS)}."
            )
        principals = [key for key in _PERMISSION_PRINCIPAL_KEYS if entry.get(key)]
        unknown = sorted(set(entry) - {"level", *_PERMISSION_PRINCIPAL_KEYS})
        if unknown:
            raise BundleInputError(
                f"--permissions-json entry {index}: unsupported keys {', '.join(unknown)}."
            )
        if len(principals) != 1:
            raise BundleInputError(
                f"--permissions-json entry {index} must name exactly one of "
                f"{', '.join(_PERMISSION_PRINCIPAL_KEYS)}."
            )
        validated.append({"level": str(level), principals[0]: str(entry[principals[0]])})
    return tuple(validated)


def resolve_bundle_inputs(cli: Mapping[str, Any], environ: Mapping[str, str]) -> BundleInputs:
    """Resolve deployment inputs: explicit CLI values, then ``KINDLING_BUNDLE_*``
    environment variables, then documented defaults. Collections replace
    rather than merge. Runtime settings are never consulted."""
    name = _require(_scalar_input(cli.get("name"), environ, "NAME"), "--name", "NAME")
    if not _NAME_PATTERN.match(name):
        raise BundleInputError("--name must match [A-Za-z0-9][A-Za-z0-9._-]*.")
    target = _require(_scalar_input(cli.get("target"), environ, "TARGET"), "--target", "TARGET")
    if not _NAME_PATTERN.match(target):
        raise BundleInputError("--target must match [A-Za-z0-9][A-Za-z0-9._-]*.")

    # Omitted: every app directory under the app roots is built.
    apps = _string_list_input(cli.get("apps") or (), environ, "APPS", "--app") or []
    for app in apps:
        if not _APP_NAME_PATTERN.match(app):
            raise BundleInputError(f"App name {app!r} must match [A-Za-z0-9][A-Za-z0-9_-]*.")
    if len(set(apps)) != len(apps):
        raise BundleInputError("The managed app set must not repeat an app name.")

    workspace_host = _require(
        _scalar_input(cli.get("workspace_host"), environ, "WORKSPACE_HOST"),
        "--workspace-host",
        "WORKSPACE_HOST",
    )
    if not workspace_host.startswith("https://"):
        raise BundleInputError("--workspace-host must be an https:// workspace URL.")
    workspace_host = workspace_host.rstrip("/")

    workspace_root = _scalar_input(
        cli.get("workspace_root"),
        environ,
        "WORKSPACE_ROOT",
        default=f"/Workspace/Shared/kindling/{name}/{target}",
    )
    assert workspace_root is not None
    if not workspace_root.startswith("/"):
        raise BundleInputError("--workspace-root must be an absolute workspace path.")

    runtime_env = _scalar_input(cli.get("env"), environ, "RUNTIME_ENV", default=target)
    assert runtime_env is not None
    if not _NAME_PATTERN.match(runtime_env):
        raise BundleInputError("--env must match [A-Za-z0-9][A-Za-z0-9._-]*.")

    wheel_values = _string_list_input(
        [str(path) for path in (cli.get("wheels") or ())], environ, "WHEELS", "--wheel"
    )
    wheels: List[Path] = []
    for raw_wheel in wheel_values or []:
        wheel_path = Path(raw_wheel).expanduser()
        if wheel_path.suffix != ".whl" or not wheel_path.is_file():
            raise BundleInputError(f"--wheel {raw_wheel!r} is not an existing .whl file.")
        wheels.append(wheel_path.resolve())
    basenames = [wheel.name for wheel in wheels]
    duplicate_names = sorted({name for name in basenames if basenames.count(name) > 1})
    if duplicate_names:
        raise BundleInputError(
            "--wheel file names must be unique; they are staged side by side under "
            f"wheels/: {', '.join(duplicate_names)}."
        )

    dependencies = _string_list_input(
        cli.get("dependencies") or (), environ, "DEPENDENCIES", "--dependency"
    )
    dependencies_defaulted = dependencies is None
    if dependencies is None:
        # Staged wheels are the dependency set unless told otherwise; the
        # unpinned PyPI default only applies when nothing else is supplied.
        dependencies = [] if wheels else list(DEFAULT_DEPENDENCIES)

    return BundleInputs(
        name=name,
        target=target,
        apps=tuple(apps),
        workspace_host=workspace_host,
        workspace_root=workspace_root,
        runtime_env=runtime_env,
        workspace_id=_scalar_input(cli.get("workspace_id"), environ, "WORKSPACE_ID"),
        catalog=_scalar_input(cli.get("catalog"), environ, "CATALOG"),
        schema=_scalar_input(cli.get("schema"), environ, "SCHEMA"),
        continuous=_bool_input(cli.get("continuous"), environ, "CONTINUOUS", default=False),
        run_as_service_principal=_scalar_input(
            cli.get("run_as_service_principal"), environ, "RUN_AS_SERVICE_PRINCIPAL"
        ),
        app_options=_validate_app_options(
            _json_input(cli.get("app_options_json"), environ, "APP_OPTIONS", "--app-options-json"),
            apps,
        ),
        permissions=_validate_permissions(
            _json_input(cli.get("permissions_json"), environ, "PERMISSIONS", "--permissions-json")
        ),
        dependencies=tuple(dependencies),
        dependencies_defaulted=dependencies_defaulted,
        wheels=tuple(wheels),
    )


# --------------------------------------------------------------------------- #
# Project inventory and configuration resolution
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ConfigSource:
    """One settings file in the runtime overlay order."""

    role: str
    path: Path
    relative: str
    exists: bool


def resolve_apps_dirs(project_root: Path, apps_dir: Optional[Path]) -> List[Path]:
    """Return the app roots to search: the explicit one, or every conventional
    root that exists (``data-apps/`` and ``apps/`` may coexist)."""
    if apps_dir is not None:
        resolved = apps_dir if apps_dir.is_absolute() else project_root / apps_dir
        if not resolved.is_dir():
            raise BundleProjectError(f"Apps directory `{resolved}` does not exist.")
        return [resolved.resolve()]
    roots = [
        (project_root / candidate).resolve()
        for candidate in DEFAULT_APP_DIR_CANDIDATES
        if (project_root / candidate).is_dir()
    ]
    if not roots:
        raise BundleProjectError(
            f"No apps directory found under `{project_root}` (looked for "
            f"{', '.join(DEFAULT_APP_DIR_CANDIDATES)}). Pass --apps-dir."
        )
    return roots


def resolve_config_dir(project_root: Path, config_dir: Optional[Path]) -> Path:
    if config_dir is not None:
        resolved = config_dir if config_dir.is_absolute() else project_root / config_dir
        if not resolved.is_dir():
            raise BundleProjectError(f"Config directory `{resolved}` does not exist.")
        return resolved.resolve()
    return (project_root / DEFAULT_CONFIG_DIR).resolve()


def resolve_app_dir(apps_dirs: Sequence[Path], app: str) -> Path:
    """Locate one app directory across the app roots; ambiguity is an error."""
    names = [app]
    snake = app.replace("-", "_")
    if snake != app:
        names.append(snake)
    candidates = [root / name for root in apps_dirs for name in names]
    matches = [candidate.resolve() for candidate in candidates if candidate.is_dir()]
    if len(matches) > 1:
        raise BundleProjectError(
            f"App '{app}' matches more than one directory: {', '.join(map(str, matches))}. "
            "Keep one, or pass --apps-dir to choose the root."
        )
    if matches:
        return matches[0]
    looked = ", ".join(str(candidate) for candidate in candidates)
    raise BundleProjectError(
        f"App '{app}' has no directory under {', '.join(map(str, apps_dirs))} "
        f"(looked at: {looked}). Every managed app needs an app directory holding "
        "its settings overlays."
    )


def resolve_config_sources(
    project_root: Path,
    config_dir: Path,
    app_dir: Path,
    env: str,
    workspace_id: Optional[str],
) -> List[ConfigSource]:
    """Return the settings files in runtime merge order.

    Mirrors ``kindling.bootstrap.download_config_files``: base, platform,
    workspace, environment, then the app's base/platform/environment overlays
    so app settings win. ``settings.local.*`` is never deployed.
    """
    platform_names = (f"settings.{PLATFORM_OVERLAY}.yaml", f"platform_{PLATFORM_OVERLAY}.yaml")
    env_names = (f"settings.{env}.yaml", f"env_{env}.yaml")
    candidates: List[Tuple[str, Path]] = [
        ("base", config_dir / "settings.yaml"),
        ("platform", _first_existing(config_dir, platform_names)),
    ]
    if workspace_id:
        candidates.append(("workspace", config_dir / f"workspace_{workspace_id}.yaml"))
    candidates.extend(
        [
            ("environment", _first_existing(config_dir, env_names)),
            ("app", app_dir / "settings.yaml"),
            ("app-platform", _first_existing(app_dir, platform_names)),
            ("app-environment", _first_existing(app_dir, env_names)),
        ]
    )
    sources: List[ConfigSource] = []
    for role, path in candidates:
        sources.append(
            ConfigSource(
                role=role,
                path=path,
                relative=_relative_posix(path, project_root),
                exists=path.is_file(),
            )
        )
    return sources


def _first_existing(directory: Path, names: Sequence[str]) -> Path:
    """The canonical file name, or the documented legacy name when only that
    exists (``platform_<p>.yaml`` / ``env_<e>.yaml``), as the runtime does."""
    for name in names:
        if (directory / name).is_file():
            return directory / name
    return directory / names[0]


def _relative_posix(path: Path, root: Path) -> str:
    try:
        return PurePosixPath(path.resolve().relative_to(root.resolve())).as_posix()
    except ValueError as exc:
        raise BundleProjectError(
            f"`{path}` is outside the project root `{root}`; bundle inputs must live "
            "inside the project."
        ) from exc


def load_settings_file(path: Path) -> Dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001 - reported to the user verbatim
        raise BundleProjectError(f"Failed to parse settings file `{path}`: {exc}") from exc
    if not isinstance(data, dict):
        raise BundleProjectError(f"Settings file `{path}` must contain a YAML mapping at root.")
    return data


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    """Deep-merge mappings, override winning; lists and scalars replace.

    The runtime merges these same files with the same rule
    (``kindling.spark_config.merge_settings_layers``; a unit test keeps them
    in agreement for plain values). Dynaconf merge markers are runtime-only
    and produce a warning here.
    """
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        # Keys match case-insensitively, as at runtime (Dynaconf lookups are
        # case-insensitive); the earlier spelling is kept.
        if key not in merged and isinstance(key, str):
            key = next((k for k in merged if isinstance(k, str) and k.lower() == key.lower()), key)
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def merge_settings(sources: Iterable[ConfigSource]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for source in sources:
        if source.exists:
            merged = deep_merge(merged, load_settings_file(source.path))
    return merged


def find_dynaconf_directives(tree: Any, prefix: str = "") -> List[str]:
    """Return dotted paths whose string values start with a Dynaconf merge
    directive. Those are applied by Dynaconf while layering files; a
    build-time merge cannot reproduce them, so the caller warns."""
    found: List[str] = []
    if isinstance(tree, Mapping):
        for key, value in tree.items():
            found.extend(find_dynaconf_directives(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(tree, list):
        for index, value in enumerate(tree):
            found.extend(find_dynaconf_directives(value, f"{prefix}[{index}]"))
    elif isinstance(tree, str) and tree.lstrip().startswith(_DYNACONF_DIRECTIVES):
        found.append(prefix)
    return found


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #

_GENERATED_HEADER = (
    "# Generated by {generator}. Do not edit: regenerate from the project's\n"
    "# settings files and deployment inputs (generator version in manifest.json)."
)

#: The template rendered when a project has none of its own. `kindling bundle
#: template init` copies it into the project as the starting point.
DEFAULT_TEMPLATE_DIR = Path(__file__).parent / "templates" / "bundle" / "databricks"
TEMPLATE_SUFFIX = ".j2"
PIPELINE_TOKEN = "__pipeline__"
SETTINGS_JSON_KEY = "kindling.lakeflow.settings_json"
CONFIG_KEYS_KEY = "kindling.lakeflow.config_keys"


@dataclass
class BuildResult:
    output_dir: Path
    files: List[str]
    manifest: Dict[str, Any]
    warnings: List[str]


def to_yaml(value: Any) -> str:
    """Render a value as YAML for embedding in a template. Mappings and lists
    come out in block style; pair with Jinja's ``indent(n)`` under a block key.
    Scalars come out as a bare YAML scalar (quoted only when needed)."""
    dumped = yaml.safe_dump(
        value, sort_keys=False, width=1_000_000, allow_unicode=True, default_flow_style=False
    )
    if dumped.endswith("...\n"):
        dumped = dumped[: -len("...\n")]
    return dumped.rstrip("\n")


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _dump_yaml(data: Mapping[str, Any]) -> str:
    return yaml.safe_dump(
        dict(data),
        sort_keys=False,
        width=1_000_000,
        allow_unicode=True,
        default_flow_style=False,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _check_output_dir(
    output_dir: Path, project_root: Path, protected: Sequence[Path], force: bool
) -> None:
    """Validate the output location before anything is rendered. Nothing is
    deleted here; ``_replace_output_dir`` swaps the finished staging directory
    in only after a successful build."""
    resolved_output = output_dir.resolve()
    resolved_root = project_root.resolve()
    if resolved_output == resolved_root or resolved_output in resolved_root.parents:
        raise BundleProjectError(
            f"Output directory `{resolved_output}` contains the project root; choose a "
            "dedicated generated directory (default dist/bundles/databricks)."
        )
    for source_dir in protected:
        resolved_source = source_dir.resolve()
        if (
            resolved_output == resolved_source
            or resolved_output in resolved_source.parents
            or resolved_source in resolved_output.parents
        ):
            raise BundleProjectError(
                f"Output directory `{resolved_output}` overlaps the project input "
                f"`{resolved_source}`; the output directory is replaced on every build, "
                "so it must not hold settings or app directories."
            )
    if resolved_output.exists():
        if not resolved_output.is_dir():
            raise BundleProjectError(f"Output path `{resolved_output}` is not a directory.")
        if any(resolved_output.iterdir()):
            if not force and not _is_generated_dir(resolved_output):
                raise BundleProjectError(
                    f"Output directory `{resolved_output}` is not empty and holds files not "
                    f"written by {GENERATOR_NAME}. Choose another --output or pass --force "
                    "to replace it."
                )


def _replace_output_dir(staging_dir: Path, output_dir: Path) -> None:
    """Atomically-enough swap: remove the previous bundle and move the
    finished staging directory into place. A failed build never touches the
    previous output, so the next build still recognizes it."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(staging_dir), str(output_dir))


def _is_generated_dir(directory: Path) -> bool:
    manifest = directory / MANIFEST_FILE
    if not manifest.is_file():
        return False
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(data, dict) and data.get("generator", {}).get("name") == GENERATOR_NAME


def _write(output_dir: Path, relative: PurePosixPath, content: str, files: List[str]) -> None:
    _reserve(relative, files)
    path = output_dir / Path(*relative.parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    files.append(relative.as_posix())


def _reserve(relative: PurePosixPath, files: List[str]) -> None:
    """Every output path is written exactly once: a template file that would
    replace a staged wheel or another rendered file is an error, not a
    silent overwrite that leaves the manifest describing different bytes."""
    if relative.as_posix() in files or relative.as_posix() == MANIFEST_FILE:
        raise BundleProjectError(
            f"Template output `{relative}` collides with a file the bundle already contains "
            "(a staged wheel, another template file, or manifest.json)."
        )


_DATABRICKS_EXTENSION_DIST = "spark-kindling-ext-databricks"


def _provides_databricks_extension(inputs: BundleInputs) -> bool:
    """True when a dependency spec, a dependency path, or a bundled wheel
    supplies the Databricks extension the generated source imports."""
    dist = _DATABRICKS_EXTENSION_DIST.replace("-", "_")
    for dependency in inputs.dependencies:
        candidate = dependency.strip().lower().replace("-", "_")
        if re.match(rf"^{dist}(\s*[=<>!~\[]|$)", candidate):
            return True
        if PurePosixPath(candidate).name.startswith(f"{dist}_") and candidate.endswith(".whl"):
            return True
    return any(wheel.name.lower().startswith(f"{dist}-") for wheel in inputs.wheels)


def _stage_wheels(
    wheels: Sequence[Path], output_dir: Path, files: List[str]
) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    for wheel in wheels:
        relative = WHEELS_DIR / wheel.name
        destination = output_dir / Path(*relative.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(wheel, destination)
        files.append(relative.as_posix())
        records.append({"file": relative.as_posix(), "sha256": _sha256(wheel)})
    return records


# --------------------------------------------------------------------------- #
# Template context
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class AppContext:
    """One app as the template sees it: its merged settings and their sources."""

    name: str
    settings: Dict[str, Any]
    settings_json: str
    sources: Tuple[ConfigSource, ...]


class KindlingHelper:
    """``kindling.*`` functions exposed to templates.

    ``configuration(app, pipes=None, extra=None)`` builds the complete
    pipeline ``configuration`` map so a template never has to know which keys
    the Lakeflow selector point-looks-up on serverless: every emitted key
    outside that default set is named in ``kindling.lakeflow.config_keys``.
    """

    def __init__(self, apps: Mapping[str, AppContext], inputs: "BundleInputs") -> None:
        self._apps = apps
        self._inputs = inputs

    def configuration(
        self,
        app: str,
        pipes: Optional[Sequence[str]] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, str]:
        context = self._apps.get(app)
        if context is None:
            raise BundleProjectError(
                f"kindling.configuration({app!r}): unknown app. Apps with settings: "
                f"{', '.join(sorted(self._apps)) or '<none>'}."
            )
        configuration: Dict[str, str] = {"kindling.data_app": app}
        pipe_ids = [str(pipe).strip() for pipe in (pipes or ()) if str(pipe).strip()]
        if pipe_ids:
            configuration["kindling.lakeflow.pipes"] = ",".join(pipe_ids)
        configuration["spark.kindling.bootstrap.environment"] = self._inputs.runtime_env
        if self._inputs.workspace_id:
            configuration["spark.kindling.bootstrap.workspace_id"] = self._inputs.workspace_id
        configuration[SETTINGS_JSON_KEY] = context.settings_json
        for key, value in (extra or {}).items():
            key = str(key)
            if key in (SETTINGS_JSON_KEY, CONFIG_KEYS_KEY):
                raise BundleProjectError(
                    f"kindling.configuration({app!r}): {key!r} is computed by the helper and "
                    "cannot be overridden."
                )
            if isinstance(value, bool):
                configuration[key] = "true" if value else "false"
            elif isinstance(value, (dict, list)):
                configuration[key] = to_json(value)
            else:
                configuration[key] = str(value)
        extra_keys = [key for key in configuration if key not in SELECTOR_DEFAULT_LOOKUP_KEYS]
        if extra_keys:
            configuration[CONFIG_KEYS_KEY] = ",".join(extra_keys)
        return configuration


def expected_config_keys(configuration: Mapping[str, Any]) -> str:
    """The ``kindling.lakeflow.config_keys`` value a configuration map must
    carry so a restricted runtime can read every key in it."""
    return ",".join(
        key
        for key in configuration
        if key not in SELECTOR_DEFAULT_LOOKUP_KEYS and key != CONFIG_KEYS_KEY
    )


class _LazyPipelines:
    """``pipelines`` in the template context; deployment inputs like catalog
    and schema are only required when a template actually iterates them."""

    def __init__(self, inputs: "BundleInputs") -> None:
        self._inputs = inputs
        self._specs: Optional[List[PipelineSpec]] = None

    def _resolve(self) -> List[PipelineSpec]:
        if self._specs is None:
            self._specs = self._inputs.pipelines()
        return self._specs

    def __iter__(self):
        return iter(self._resolve())

    def __len__(self) -> int:
        return len(self._resolve())

    def __bool__(self) -> bool:
        return bool(self._resolve())

    def __getitem__(self, index):
        return self._resolve()[index]


def _resolve_template_dir(project_root: Path, template_dir: Optional[Path]) -> Tuple[Path, str]:
    if template_dir is None:
        return DEFAULT_TEMPLATE_DIR, "builtin"
    resolved = template_dir if template_dir.is_absolute() else project_root / template_dir
    resolved = resolved.resolve()
    if not resolved.is_dir():
        raise BundleProjectError(f"Template directory `{resolved}` does not exist.")
    try:
        label = PurePosixPath(resolved.relative_to(project_root.resolve())).as_posix()
    except ValueError:
        label = str(resolved)
    return resolved, label


def _template_files(template_dir: Path) -> List[Path]:
    return sorted(path for path in template_dir.rglob("*") if path.is_file())


def _jinja_environment(template_dir: Path) -> jinja2.Environment:
    # These templates produce YAML, not HTML: autoescaping would HTML-escape
    # quotes and ampersands inside YAML scalars and corrupt the bundle.
    environment = jinja2.Environment(  # nosec B701
        loader=jinja2.FileSystemLoader(str(template_dir)),
        undefined=jinja2.StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
        autoescape=False,
    )
    environment.filters["to_yaml"] = to_yaml
    environment.filters["to_json"] = to_json
    return environment


def render_templates(
    template_dir: Path,
    context: Mapping[str, Any],
    output_dir: Path,
    files: List[str],
) -> List[str]:
    """Render every ``*.j2`` file (once per pipeline for ``[pipeline]`` names),
    copy everything else verbatim, and return the rendered relative paths."""
    environment = _jinja_environment(template_dir)
    rendered: List[str] = []
    for source in _template_files(template_dir):
        relative = PurePosixPath(source.relative_to(template_dir).as_posix())
        if relative.name == "README.md" and relative.parent == PurePosixPath("."):
            continue  # documents the template itself, not part of a bundle
        if not source.name.endswith(TEMPLATE_SUFFIX):
            _reserve(relative, files)
            destination = output_dir / Path(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            files.append(relative.as_posix())
            rendered.append(relative.as_posix())  # validated like rendered output
            continue
        output_name = relative.name[: -len(TEMPLATE_SUFFIX)]
        template = environment.get_template(relative.as_posix())
        if PIPELINE_TOKEN in output_name:
            for spec in context["pipelines"]:
                target = relative.parent / output_name.replace(PIPELINE_TOKEN, spec.key)
                content = _render_one(template, {**context, "pipeline": spec}, relative)
                _write(output_dir, target, content, files)
                rendered.append(target.as_posix())
        else:
            target = relative.parent / output_name
            content = _render_one(template, dict(context), relative)
            _write(output_dir, target, content, files)
            rendered.append(target.as_posix())
    return rendered


def _render_one(template: Any, context: Mapping[str, Any], relative: PurePosixPath) -> str:
    try:
        content = template.render(**context)
    except BundleError:
        raise
    except jinja2.TemplateError as exc:
        raise BundleProjectError(f"Template `{relative}` failed to render: {exc}") from exc
    return content if content.endswith("\n") else content + "\n"


def _validate_rendered_pipelines(
    output_dir: Path, rendered: Sequence[str], apps: Mapping[str, AppContext]
) -> List[Dict[str, Any]]:
    """Parse the rendered resource files and check every pipeline's
    configuration was produced consistently: inline settings present and
    ``config_keys`` covering every non-default key. Returns manifest records."""
    records: List[Dict[str, Any]] = []
    for relative in rendered:
        if not relative.endswith((".yml", ".yaml")):
            continue
        path = output_dir / Path(*PurePosixPath(relative).parts)
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise BundleProjectError(f"Rendered `{relative}` is not valid YAML: {exc}") from exc
        pipelines = ((document or {}).get("resources") or {}).get("pipelines") or {}
        for key, pipeline in pipelines.items():
            configuration = (pipeline or {}).get("configuration") or {}
            if not isinstance(configuration, dict):
                raise BundleProjectError(
                    f"Pipeline `{key}` in `{relative}`: configuration must be a mapping."
                )
            if SETTINGS_JSON_KEY not in configuration:
                raise BundleProjectError(
                    f"Pipeline `{key}` in `{relative}` has no {SETTINGS_JSON_KEY}. Build its "
                    "configuration with kindling.configuration(<app>, ...) in the template."
                )
            non_string = [k for k, v in configuration.items() if not isinstance(v, str)]
            if non_string:
                raise BundleProjectError(
                    f"Pipeline `{key}` in `{relative}`: configuration values must be strings "
                    f"({', '.join(non_string)})."
                )
            expected = expected_config_keys(configuration)
            actual = configuration.get(CONFIG_KEYS_KEY, "")
            if set(filter(None, actual.split(","))) != set(filter(None, expected.split(","))):
                raise BundleProjectError(
                    f"Pipeline `{key}` in `{relative}`: {CONFIG_KEYS_KEY} must name every "
                    f"non-default key (expected {expected!r}, got {actual!r}). Restricted "
                    "runtimes cannot read unnamed keys; use kindling.configuration()."
                )
            app = configuration.get("kindling.data_app", "")
            if app not in apps:
                raise BundleProjectError(
                    f"Pipeline `{key}` in `{relative}` selects app {app!r}, which has no "
                    "settings in this project."
                )
            if configuration[SETTINGS_JSON_KEY] != apps[app].settings_json:
                raise BundleProjectError(
                    f"Pipeline `{key}` in `{relative}`: {SETTINGS_JSON_KEY} is not the merged "
                    f"settings of app {app!r}. Build the configuration with "
                    "kindling.configuration() instead of writing it by hand."
                )
            records.append(
                {
                    "key": key,
                    "name": pipeline.get("name"),
                    "app": app,
                    "resource_file": relative,
                    "catalog": pipeline.get("catalog"),
                    "schema": pipeline.get("schema", pipeline.get("target")),
                    "continuous": bool(pipeline.get("continuous", False)),
                    "pipes": [
                        pipe
                        for pipe in configuration.get("kindling.lakeflow.pipes", "").split(",")
                        if pipe
                    ],
                    "settings_json_bytes": len(configuration[SETTINGS_JSON_KEY].encode("utf-8")),
                }
            )
    return records


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #


def discover_apps(apps_dirs: Sequence[Path]) -> List[str]:
    """Every app directory under the app roots (used when --app is omitted)."""
    names: List[str] = []
    for root in apps_dirs:
        for child in sorted(root.iterdir()):
            if child.is_dir() and not child.name.startswith((".", "_")) and child.name not in names:
                names.append(child.name)
    return names


def _app_contexts(
    inputs: BundleInputs,
    project_root: Path,
    config_dir: Path,
    apps_dirs: Sequence[Path],
    warnings: List[str],
) -> Dict[str, AppContext]:
    names = list(inputs.apps) or discover_apps(apps_dirs)
    if not names:
        raise BundleProjectError(
            f"No apps found under {', '.join(map(str, apps_dirs))}; pass --app or add an app "
            "directory with settings."
        )
    contexts: Dict[str, AppContext] = {}
    for name in names:
        app_dir = resolve_app_dir(apps_dirs, name)
        sources = resolve_config_sources(
            project_root, config_dir, app_dir, inputs.runtime_env, inputs.workspace_id
        )
        if not any(source.exists for source in sources):
            looked = ", ".join(source.relative for source in sources)
            raise BundleProjectError(
                f"No settings files found for app '{name}' (looked at: {looked})."
            )
        settings = merge_settings(sources)
        directives = find_dynaconf_directives(settings)
        if directives:
            warnings.append(
                f"App '{name}': Dynaconf merge directives are not applied by the build-time "
                f"merge and are passed through literally: {', '.join(directives)}."
            )
        contexts[name] = AppContext(
            name=name,
            settings=settings,
            settings_json=to_json(settings),
            sources=tuple(sources),
        )
    return contexts


def build_bundle(
    inputs: BundleInputs,
    project_root: Path,
    output_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
    apps_dir: Optional[Path] = None,
    template_dir: Optional[Path] = None,
    force: bool = False,
) -> BuildResult:
    """Render the bundle. Same inputs, template and generator version produce
    identical file contents (no timestamps, stable ordering)."""
    project_root = project_root.resolve()
    if not project_root.is_dir():
        raise BundleProjectError(f"Project root `{project_root}` is not a directory.")
    resolved_output = (
        output_dir if output_dir is not None else project_root / Path(*DEFAULT_OUTPUT_SUBDIR.parts)
    )
    if not resolved_output.is_absolute():
        resolved_output = project_root / resolved_output

    warnings: List[str] = []
    if inputs.dependencies_defaulted and not inputs.wheels:
        warnings.append(
            "No --dependency / KINDLING_BUNDLE_DEPENDENCIES given; the pipeline environment "
            f"depends on {', '.join(DEFAULT_DEPENDENCIES)} unpinned. Pin exact versions for "
            "reproducible promotion."
        )
    if not _provides_databricks_extension(inputs):
        warnings.append(
            "Neither --dependency nor --wheel supplies spark-kindling-ext-databricks; the "
            "generated pipeline source imports kindling_ext_databricks and will fail to start."
        )

    # Resolve everything before writing anything.
    resolved_config_dir = resolve_config_dir(project_root, config_dir)
    resolved_apps_dirs = resolve_apps_dirs(project_root, apps_dir)
    resolved_template_dir, template_label = _resolve_template_dir(project_root, template_dir)
    apps = _app_contexts(inputs, project_root, resolved_config_dir, resolved_apps_dirs, warnings)
    template_files = _template_files(resolved_template_dir)
    if not template_files:
        raise BundleProjectError(f"Template directory `{resolved_template_dir}` is empty.")

    protected = [resolved_config_dir, *resolved_apps_dirs]
    protected.extend(source.path.parent for app in apps.values() for source in app.sources)
    if template_label != "builtin":
        protected.append(resolved_template_dir)
    _check_output_dir(resolved_output, project_root, protected, force)

    # Render into a sibling staging directory and swap it in only on success.
    resolved_output.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(
        tempfile.mkdtemp(prefix=f".{resolved_output.name}.building-", dir=resolved_output.parent)
    )
    try:
        manifest, files, warnings = _render_bundle(
            inputs,
            resolved_template_dir,
            template_label,
            template_files,
            apps,
            staging_dir,
            warnings,
        )
        _replace_output_dir(staging_dir, resolved_output)
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
    return BuildResult(
        output_dir=resolved_output, files=sorted(files), manifest=manifest, warnings=warnings
    )


def _render_bundle(
    inputs: BundleInputs,
    resolved_template_dir: Path,
    template_label: str,
    template_files: Sequence[Path],
    apps: Mapping[str, AppContext],
    resolved_output: Path,
    warnings: List[str],
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    files: List[str] = []
    version = _kindling_version()
    wheel_records = _stage_wheels(inputs.wheels, resolved_output, files)
    wheel_files = [PurePosixPath(record["file"]).name for record in wheel_records]
    dependencies = list(inputs.dependencies) + [
        (PurePosixPath("..") / WHEELS_DIR / wheel).as_posix() for wheel in wheel_files
    ]

    context: Dict[str, Any] = {
        "generated_header": _GENERATED_HEADER.format(generator=GENERATOR_NAME),
        "kindling_version": version,
        "bundle": {
            "name": inputs.name,
            "target": inputs.target,
            "runtime_env": inputs.runtime_env,
            "workspace_host": inputs.workspace_host,
            "workspace_root": inputs.workspace_root,
            "workspace_id": inputs.workspace_id,
            "run_as_service_principal": inputs.run_as_service_principal,
            "permissions": [dict(entry) for entry in inputs.permissions],
        },
        "apps": apps,
        # Apps discovered because --app was omitted drive the default
        # template's pipelines exactly as named apps would.
        "pipelines": _LazyPipelines(
            inputs if inputs.apps else dataclasses.replace(inputs, apps=tuple(apps))
        ),
        "dependencies": dependencies,
        "wheels": wheel_files,
        "kindling": KindlingHelper(apps, inputs),
    }
    rendered = render_templates(resolved_template_dir, context, resolved_output, files)
    pipeline_records = _validate_rendered_pipelines(resolved_output, rendered, apps)
    if not pipeline_records:
        raise BundleProjectError(
            f"Template `{template_label}` rendered no pipeline resources under resources/*."
        )
    for record in pipeline_records:
        if record["settings_json_bytes"] > INLINE_SETTINGS_WARN_BYTES:
            warnings.append(
                f"Pipeline '{record['name']}': inline settings are "
                f"{record['settings_json_bytes']} bytes. Databricks documents no "
                "pipeline-configuration value limit; verify the deployed pipeline reads it."
            )

    manifest: Dict[str, Any] = {
        "generator": {"name": GENERATOR_NAME, "version": version},
        "bundle": {
            "name": inputs.name,
            "target": inputs.target,
            "runtime_environment": inputs.runtime_env,
            "workspace_host": inputs.workspace_host,
            "workspace_root": inputs.workspace_root,
            "workspace_id": inputs.workspace_id,
            "run_as_service_principal": inputs.run_as_service_principal,
            "serverless": True,
        },
        "template": {
            "source": template_label,
            "files": [
                {
                    "path": PurePosixPath(
                        path.relative_to(resolved_template_dir).as_posix()
                    ).as_posix(),
                    "sha256": _sha256(path),
                }
                for path in template_files
            ],
        },
        "apps": [
            {
                "name": app.name,
                "config_sources": [
                    {"role": source.role, "path": source.relative, "sha256": _sha256(source.path)}
                    for source in app.sources
                    if source.exists
                ],
            }
            for app in apps.values()
        ],
        "pipelines": pipeline_records,
        "dependencies": list(inputs.dependencies),
        "wheels": wheel_records,
        "permissions": [dict(entry) for entry in inputs.permissions],
        "warnings": list(warnings),
    }
    manifest_path = resolved_output / MANIFEST_FILE
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    files.append(MANIFEST_FILE)
    return manifest, files, warnings


def init_template(
    project_root: Path, destination: Optional[Path] = None, force: bool = False
) -> Path:
    """Copy the default template into a project as its own starting point."""
    project_root = project_root.resolve()
    target = destination if destination is not None else project_root / "bundle-template"
    if not target.is_absolute():
        target = project_root / target
    target = target.resolve()
    if target == project_root or target in project_root.parents:
        raise BundleProjectError(
            f"`{target}` is the project root or one of its parents; the template must live in "
            "its own directory (default bundle-template/)."
        )
    if target.exists():
        if not target.is_dir():
            raise BundleProjectError(f"`{target}` exists and is not a directory.")
        if any(target.iterdir()):
            if not force:
                raise BundleProjectError(
                    f"`{target}` already exists and is not empty; pass --force to overwrite it."
                )
            shutil.rmtree(target)
    shutil.copytree(DEFAULT_TEMPLATE_DIR, target, dirs_exist_ok=True)
    return target
