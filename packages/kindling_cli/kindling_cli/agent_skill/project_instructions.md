# Kindling domain project

This repository is a Kindling domain project. Before writing or changing
entities, pipes, apps or `settings*.yaml`, use the **kindling** skill (it
covers the API, the CLI workflow and the rules below in detail).

- Scaffold with the CLI (`kindling package add entity|pipe|ingestion`,
  `kindling app init`) so files land where the runtime imports them:
  `packages/<pkg>/src/<pkg>/entities/` and `pipes/`.
- Validate from the repo root with
  `kindling app validate --app apps/<app>/app.py --env local`; test a package
  with `uv run poe test` inside it.
- Never run a bare `uv sync` inside a package directory; use `uv run` there and
  `uv sync --all-packages` at the root.
- Keep platform and environment differences in `settings.yaml`, never in code.
