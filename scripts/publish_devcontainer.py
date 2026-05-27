#!/usr/bin/env python3
"""Build and publish the Kindling devcontainer image to GHCR.

    poe publish-devcontainer                  # build + push current version
    poe publish-devcontainer --no-push        # build only (local smoke-test)
    poe publish-devcontainer --version 0.9.33 # override version tag
"""

import base64
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REGISTRY = "ghcr.io"
REPO = "sep/spark-kindling-framework"
IMAGE = f"{REGISTRY}/{REPO}/devcontainer"
DOCKERFILE = Path(".github/Dockerfile.devcontainer")


def _current_version() -> str:
    content = (Path(__file__).parent.parent / "pyproject.toml").read_text()
    match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find version in pyproject.toml")
    return match.group(1)


def _gh_actor() -> str:
    result = subprocess.run(
        ["gh", "api", "user", "-q", ".login"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Could not get GitHub user via 'gh api user'. Run 'gh auth login' first."
        )
    return result.stdout.strip()


def _check_gh_scopes() -> None:
    """Verify the gh token has write:packages scope before attempting a push."""
    result = subprocess.run(
        ["gh", "auth", "status"],
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    if "write:packages" not in output:
        raise RuntimeError(
            "GitHub token is missing the 'write:packages' scope.\n"
            "Run: gh auth refresh --scopes write:packages"
        )


def _make_docker_config(tmpdir: str) -> dict:
    """Write a minimal Docker config that avoids the host credential store.

    On WSL2/devcontainers the host Docker config often points to
    docker-credential-wincred, which is not available inside Linux containers.
    Writing a fresh config.json with the auth encoded inline sidesteps it.
    """
    actor = _gh_actor()
    token_result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True)
    if token_result.returncode != 0:
        raise RuntimeError(
            "Could not get GitHub token via 'gh auth token'. Run 'gh auth login' first."
        )
    token = token_result.stdout.strip()

    auth = base64.b64encode(f"{actor}:{token}".encode()).decode()
    config = {"auths": {REGISTRY: {"auth": auth}}}
    config_path = Path(tmpdir) / "config.json"
    config_path.write_text(json.dumps(config))
    return {"DOCKER_CONFIG": tmpdir}


BUILDER_NAME = "kindling-devcontainer-builder"


def _ensure_builder(env: dict) -> None:
    """Ensure a docker-container builder exists; the default driver lacks registry cache support."""
    inspect = subprocess.run(
        ["docker", "buildx", "inspect", BUILDER_NAME],
        capture_output=True,
        env=env,
    )
    if inspect.returncode != 0:
        print(f"Creating buildx builder '{BUILDER_NAME}' (docker-container driver)...")
        subprocess.run(
            ["docker", "buildx", "create", "--name", BUILDER_NAME, "--driver", "docker-container"],
            check=True,
            env=env,
        )


def main(version: str = "", push: bool = True) -> int:
    try:
        resolved_version = version or _current_version()
        print(f"Building devcontainer image version {resolved_version}")

        env = os.environ.copy()

        with tempfile.TemporaryDirectory() as tmpdir:
            if push:
                _check_gh_scopes()
                docker_env = _make_docker_config(tmpdir)
                env.update(docker_env)
                _ensure_builder(env)

            build_cmd = [
                "docker",
                "buildx",
                "build",
                "--file",
                str(DOCKERFILE),
                "--build-arg",
                f"KINDLING_VERSION={resolved_version}",
                "--tag",
                f"{IMAGE}:{resolved_version}",
                "--tag",
                f"{IMAGE}:latest",
            ]
            if push:
                build_cmd += [
                    "--builder",
                    BUILDER_NAME,
                    "--cache-from",
                    f"type=registry,ref={IMAGE}:buildcache",
                    "--cache-to",
                    f"type=registry,ref={IMAGE}:buildcache,mode=max",
                    "--push",
                ]
            build_cmd.append(".")

            print(f"Running: {' '.join(build_cmd)}")
            result = subprocess.run(build_cmd, env=env)

        if result.returncode != 0:
            print("docker buildx build failed")
            return result.returncode

        if push:
            print(f"Published: {IMAGE}:{resolved_version}")
            print(f"Published: {IMAGE}:latest")
        else:
            print(f"Built (local): {IMAGE}:{resolved_version}")
        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build and publish the Kindling devcontainer image"
    )
    parser.add_argument(
        "--version", default="", help="Image version tag (default: current pyproject version)"
    )
    parser.add_argument(
        "--no-push", dest="push", action="store_false", help="Build only, do not push"
    )
    args = parser.parse_args()
    sys.exit(main(version=args.version, push=args.push))
