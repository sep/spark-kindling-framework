"""Build a PEP 503 simple index from wheels in /opt/kindling-packages/wheels/."""

import hashlib
import pathlib
import re

wheels_dir = pathlib.Path("/opt/kindling-packages/wheels")
simple_dir = pathlib.Path("/opt/kindling-packages/simple")
simple_dir.mkdir(parents=True)

packages: dict = {}
for whl in sorted(wheels_dir.glob("*.whl")):
    raw = whl.name.split("-")[0]
    pkg = re.sub(r"[-_.]+", "-", raw).lower()
    packages.setdefault(pkg, []).append(whl)

links = "\n".join(f'<a href="{name}/">{name}</a>' for name in sorted(packages))
(simple_dir / "index.html").write_text(f"<!DOCTYPE html><html><body>\n{links}\n</body></html>\n")

for pkg_name, wheels in packages.items():
    pkg_dir = simple_dir / pkg_name
    pkg_dir.mkdir(exist_ok=True)
    entries = []
    for whl in wheels:
        digest = hashlib.sha256(whl.read_bytes()).hexdigest()
        entries.append(f'<a href="file://{whl}#sha256={digest}">{whl.name}</a>')
    body = "\n".join(entries)
    (pkg_dir / "index.html").write_text(f"<!DOCTYPE html><html><body>\n{body}\n</body></html>\n")

print(f"PEP 503 index created: {len(packages)} package(s)")
