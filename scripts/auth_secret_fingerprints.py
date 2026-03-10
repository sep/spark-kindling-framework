#!/usr/bin/env python3
"""
Print one-way fingerprints for Azure auth values from .env or the current environment.
"""

import argparse
import hashlib
import os
import sys
from typing import Iterable


DEFAULT_KEYS = ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET")


def _load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f".env file not found: {path}")

    with open(path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ[key] = value


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _print_secret_flags(name: str, value: str) -> None:
    print(
        f"{name} flags: "
        f"has_newline={'yes' if chr(10) in value else 'no'} "
        f"has_carriage_return={'yes' if chr(13) in value else 'no'} "
        f"leading_whitespace={'yes' if value[:1].isspace() else 'no'} "
        f"trailing_whitespace={'yes' if value[-1:].isspace() else 'no'}"
    )


def _iter_keys(raw_keys: str) -> Iterable[str]:
    if not raw_keys.strip():
        return DEFAULT_KEYS
    return tuple(key.strip() for key in raw_keys.split(",") if key.strip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print SHA-256 fingerprints for Azure auth env vars."
    )
    parser.add_argument(
        "--dotenv",
        default=".env",
        help="Path to .env file to load before fingerprinting. Default: .env",
    )
    parser.add_argument(
        "--skip-dotenv",
        action="store_true",
        help="Do not load a .env file; inspect the current environment only.",
    )
    parser.add_argument(
        "--keys",
        default=",".join(DEFAULT_KEYS),
        help="Comma-separated env var names to fingerprint.",
    )
    args = parser.parse_args()

    try:
        if not args.skip_dotenv:
            _load_dotenv(args.dotenv)
    except FileNotFoundError as exc:
        print(f"❌ {exc}")
        return 1

    keys = tuple(_iter_keys(args.keys))
    if not keys:
        print("❌ No keys specified")
        return 1

    for key in keys:
        value = os.getenv(key, "")
        digest = _fingerprint(value) if value else "<missing>"
        print(f"{key}: len={len(value)} sha256={digest}")
        if "SECRET" in key:
            _print_secret_flags(key, value)

    return 0


if __name__ == "__main__":
    sys.exit(main())
