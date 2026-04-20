# spark-kindling-cli

Command-line tooling for the Spark Kindling Framework. Distributed on PyPI as
`spark-kindling-cli`; installs the `kindling` console script.

## Install

```bash
pip install spark-kindling-cli
```

Depends on `spark-kindling-sdk` for the underlying platform operations; install
it explicitly if you want to use the SDK directly.

## Commands

- `kindling config init` — generate a starter `settings.yaml`
- `kindling config set <key> <value>` — set a config value using dot-notation
- `kindling env check` — validate the local Python/config environment
- `kindling workspace check` — validate the configured platform workspace (auto-detects platform from env vars)
- `kindling workspace init` — scaffold bootstrap + starter notebook files for a platform
- `kindling workspace deploy` — upload wheels, bootstrap, config, and notebooks to the configured artifacts storage

Run any command with `--help` for full options.

## Related

- Runtime framework: `pip install 'spark-kindling[<platform>]'` where `<platform>` is one of `synapse`, `databricks`, `fabric`, `standalone`.
- Design-time SDK: `pip install spark-kindling-sdk`.
