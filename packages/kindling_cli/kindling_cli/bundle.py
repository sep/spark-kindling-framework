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

import hashlib
import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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
    if not isinstance(parsed, list) or not all(isinstance(item, str) and item for item in parsed):
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

    apps = _string_list_input(cli.get("apps") or (), environ, "APPS", "--app")
    if not apps:
        raise BundleInputError(
            f"Missing deployment input: pass --app (repeatable) or set {ENV_PREFIX}APPS "
            "to a JSON string array naming the complete managed app set."
        )
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


def resolve_apps_dir(project_root: Path, apps_dir: Optional[Path]) -> Path:
    if apps_dir is not None:
        resolved = apps_dir if apps_dir.is_absolute() else project_root / apps_dir
        if not resolved.is_dir():
            raise BundleProjectError(f"Apps directory `{resolved}` does not exist.")
        return resolved.resolve()
    for candidate in DEFAULT_APP_DIR_CANDIDATES:
        resolved = project_root / candidate
        if resolved.is_dir():
            return resolved.resolve()
    raise BundleProjectError(
        f"No apps directory found under `{project_root}` (looked for "
        f"{', '.join(DEFAULT_APP_DIR_CANDIDATES)}). Pass --apps-dir."
    )


def resolve_config_dir(project_root: Path, config_dir: Optional[Path]) -> Path:
    if config_dir is not None:
        resolved = config_dir if config_dir.is_absolute() else project_root / config_dir
        if not resolved.is_dir():
            raise BundleProjectError(f"Config directory `{resolved}` does not exist.")
        return resolved.resolve()
    return (project_root / DEFAULT_CONFIG_DIR).resolve()


def resolve_app_dir(apps_dir: Path, app: str) -> Path:
    candidates = [apps_dir / app]
    snake = app.replace("-", "_")
    if snake != app:
        candidates.append(apps_dir / snake)
    for candidate in candidates:
        if candidate.is_dir():
            return candidate.resolve()
    looked = ", ".join(str(candidate) for candidate in candidates)
    raise BundleProjectError(
        f"App '{app}' has no directory under `{apps_dir}` (looked at: {looked}). "
        "Every managed app needs an app directory holding its settings overlays."
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
    candidates: List[Tuple[str, Path]] = [
        ("base", config_dir / "settings.yaml"),
        ("platform", config_dir / f"settings.{PLATFORM_OVERLAY}.yaml"),
    ]
    if workspace_id:
        candidates.append(("workspace", config_dir / f"workspace_{workspace_id}.yaml"))
    candidates.extend(
        [
            ("environment", config_dir / f"settings.{env}.yaml"),
            ("app", app_dir / "settings.yaml"),
            ("app-platform", app_dir / f"settings.{PLATFORM_OVERLAY}.yaml"),
            ("app-environment", app_dir / f"settings.{env}.yaml"),
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

    Matches Dynaconf's ``MERGE_ENABLED_FOR_DYNACONF`` behaviour for plain
    YAML values, which is how the runtime layers these same files.
    """
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
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
    "# settings files and deployment inputs (generator version in manifest.json).\n"
)

_SOURCE_TEMPLATE = '''"""Generic Lakeflow pipeline source generated by {generator}.

The pipeline configuration selects the Kindling data app and carries its
effective settings; this file only hands control to the selector.
"""

from kindling_ext_databricks.lakeflow_app_selector import declare_from_pipeline_config

declare_from_pipeline_config()
'''


@dataclass
class BuildResult:
    output_dir: Path
    files: List[str]
    manifest: Dict[str, Any]
    warnings: List[str]


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


def _prepare_output_dir(output_dir: Path, project_root: Path, force: bool) -> None:
    resolved_output = output_dir.resolve()
    resolved_root = project_root.resolve()
    if resolved_output == resolved_root or resolved_output in resolved_root.parents:
        raise BundleProjectError(
            f"Output directory `{resolved_output}` contains the project root; choose a "
            "dedicated generated directory (default dist/bundles/databricks)."
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
            shutil.rmtree(resolved_output)
    resolved_output.mkdir(parents=True, exist_ok=True)


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
    path = output_dir / Path(*relative.parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    files.append(relative.as_posix())


def _pipeline_configuration(
    spec: PipelineSpec, inputs: BundleInputs, settings: Mapping[str, Any]
) -> Dict[str, str]:
    configuration: Dict[str, str] = {"kindling.data_app": spec.app}
    if spec.pipes:
        configuration["kindling.lakeflow.pipes"] = ",".join(spec.pipes)
    configuration["spark.kindling.bootstrap.environment"] = inputs.runtime_env
    if inputs.workspace_id:
        configuration["spark.kindling.bootstrap.workspace_id"] = inputs.workspace_id
    configuration["kindling.lakeflow.settings_json"] = json.dumps(
        settings, ensure_ascii=False, separators=(",", ":")
    )
    # Restricted runtimes only point-look-up the selector's default keys, so
    # every other key written here must be named for it to be read at all.
    extra_keys = [key for key in configuration if key not in SELECTOR_DEFAULT_LOOKUP_KEYS]
    if extra_keys:
        configuration["kindling.lakeflow.config_keys"] = ",".join(extra_keys)
    return configuration


def _pipeline_resource(
    spec: PipelineSpec,
    inputs: BundleInputs,
    configuration: Mapping[str, str],
    wheel_files: Sequence[str],
) -> Dict[str, Any]:
    source_path = PurePosixPath("..") / SOURCE_FILE
    dependencies = list(inputs.dependencies) + [
        (PurePosixPath("..") / WHEELS_DIR / wheel).as_posix() for wheel in wheel_files
    ]
    pipeline: Dict[str, Any] = {
        "name": spec.name,
        "serverless": True,
        "catalog": spec.catalog,
        "target": spec.schema,
        "continuous": spec.continuous,
        "libraries": [{"file": {"path": source_path.as_posix()}}],
        "environment": {"dependencies": dependencies},
        "configuration": dict(configuration),
    }
    if inputs.permissions:
        pipeline["permissions"] = [dict(entry) for entry in inputs.permissions]
    return {"resources": {"pipelines": {spec.key: pipeline}}}


def _bundle_root(inputs: BundleInputs, sync_include: Sequence[str]) -> Dict[str, Any]:
    target: Dict[str, Any] = {
        "default": True,
        "workspace": {"host": inputs.workspace_host, "root_path": inputs.workspace_root},
    }
    if inputs.run_as_service_principal:
        target["run_as"] = {"service_principal_name": inputs.run_as_service_principal}
    return {
        "bundle": {"name": inputs.name},
        "include": [f"{RESOURCES_DIR.as_posix()}/*.pipeline.yml"],
        "sync": {"include": list(sync_include)},
        "targets": {inputs.target: target},
    }


PipelinePlan = Tuple[PipelineSpec, List[ConfigSource], Dict[str, Any]]

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


def _plan_pipelines(
    inputs: BundleInputs,
    project_root: Path,
    config_dir: Path,
    apps_dir: Path,
    warnings: List[str],
) -> List[PipelinePlan]:
    """Resolve every pipeline's app directory, settings sources and (for the
    inline transport) merged settings before anything is written."""
    plan: List[PipelinePlan] = []
    for spec in inputs.pipelines():
        app_dir = resolve_app_dir(apps_dir, spec.app)
        sources = resolve_config_sources(
            project_root, config_dir, app_dir, inputs.runtime_env, inputs.workspace_id
        )
        if not any(source.exists for source in sources):
            looked = ", ".join(source.relative for source in sources)
            raise BundleProjectError(
                f"No settings files found for app '{spec.app}' (looked at: {looked})."
            )
        settings = merge_settings(sources)
        directives = find_dynaconf_directives(settings)
        if directives:
            warnings.append(
                f"Pipeline '{spec.name}': Dynaconf merge directives are not applied by the "
                f"build-time merge and are passed through literally: {', '.join(directives)}."
            )
        plan.append((spec, sources, settings))
    return plan


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


def _pipeline_record(
    spec: PipelineSpec,
    sources: Sequence[ConfigSource],
    configuration: Mapping[str, str],
    warnings: List[str],
) -> Dict[str, Any]:
    record: Dict[str, Any] = {
        "key": spec.key,
        "name": spec.name,
        "app": spec.app,
        "resource_file": spec.resource_file.as_posix(),
        "catalog": spec.catalog,
        "schema": spec.schema,
        "continuous": spec.continuous,
        "pipes": list(spec.pipes),
        "config_sources": [
            {"role": source.role, "path": source.relative, "sha256": _sha256(source.path)}
            for source in sources
            if source.exists
        ],
    }
    size = len(configuration["kindling.lakeflow.settings_json"].encode("utf-8"))
    record["settings_json_bytes"] = size
    if size > INLINE_SETTINGS_WARN_BYTES:
        warnings.append(
            f"Pipeline '{spec.name}': inline settings are {size} bytes. Databricks documents "
            "no pipeline-configuration value limit; verify the deployed pipeline reads it."
        )
    return record


def build_bundle(
    inputs: BundleInputs,
    project_root: Path,
    output_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
    apps_dir: Optional[Path] = None,
    force: bool = False,
) -> BuildResult:
    """Assemble the bundle directory. Same inputs and generator version
    produce identical file contents (no timestamps, stable ordering)."""
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
    plan = _plan_pipelines(
        inputs,
        project_root,
        resolve_config_dir(project_root, config_dir),
        resolve_apps_dir(project_root, apps_dir),
        warnings,
    )

    _prepare_output_dir(resolved_output, project_root, force)
    files: List[str] = []
    version = _kindling_version()
    header = _GENERATED_HEADER.format(generator=GENERATOR_NAME)

    _write(
        resolved_output,
        SOURCE_FILE,
        _SOURCE_TEMPLATE.format(generator=GENERATOR_NAME),
        files,
    )
    wheel_records = _stage_wheels(inputs.wheels, resolved_output, files)
    wheel_files = [PurePosixPath(record["file"]).name for record in wheel_records]

    sync_include = [f"{SOURCE_FILE.parent.as_posix()}/**"]

    pipeline_records: List[Dict[str, Any]] = []
    for spec, sources, settings in plan:
        configuration = _pipeline_configuration(spec, inputs, settings)
        resource = _pipeline_resource(spec, inputs, configuration, wheel_files)
        _write(resolved_output, spec.resource_file, header + _dump_yaml(resource), files)
        pipeline_records.append(_pipeline_record(spec, sources, configuration, warnings))

    _write(
        resolved_output,
        PurePosixPath("databricks.yml"),
        header + _dump_yaml(_bundle_root(inputs, sync_include)),
        files,
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
        "apps": list(inputs.apps),
        "pipelines": pipeline_records,
        "dependencies": list(inputs.dependencies),
        "wheels": wheel_records,
        "permissions": [dict(entry) for entry in inputs.permissions],
        "warnings": list(warnings),
    }
    _write(
        resolved_output,
        PurePosixPath(MANIFEST_FILE),
        json.dumps(manifest, indent=2, sort_keys=False) + "\n",
        files,
    )
    return BuildResult(
        output_dir=resolved_output, files=sorted(files), manifest=manifest, warnings=warnings
    )
