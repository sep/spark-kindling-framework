# Obsolete proposals

Proposals in this folder are archived because the work they specified has since shipped or been superseded. They're kept in-tree rather than deleted for historical reference — use them to understand the design thinking behind features that are now in the codebase.

| Proposal | Status | What satisfies it today |
|---|---|---|
| [blinker_events_implementation_plan.md](./blinker_events_implementation_plan.md) | Implemented | [packages/kindling/signaling.py](../../../packages/kindling/signaling.py) — `SignalPayload`, `BlinkerSignalProvider` |
| [config_based_entity_providers.md](./config_based_entity_providers.md) | Implemented | [packages/kindling/entity_resolution.py](../../../packages/kindling/entity_resolution.py) — `ConfigDrivenEntityNameMapper`, `ConfigDrivenEntityPathLocator` |
| [dag_execution_implementation_plan.md](./dag_execution_implementation_plan.md) | Implemented | [pipe_graph.py](../../../packages/kindling/pipe_graph.py), [execution_strategy.py](../../../packages/kindling/execution_strategy.py), [generation_executor.py](../../../packages/kindling/generation_executor.py) |
| [databricks_uc_vs_classic_capability_plan.md](./databricks_uc_vs_classic_capability_plan.md) | Implemented | [packages/kindling/features.py](../../../packages/kindling/features.py) — `databricks.uc_enabled`, `databricks.volumes_enabled` feature flags |
| [domain_package_development.md](./domain_package_development.md) | Implemented | `kindling new` CLI (PR #57, shipped v0.9.0) |
| [local_bootstrap_plan.md](./local_bootstrap_plan.md) | Implemented | Standalone as first-class platform in [bootstrap.py](../../../packages/kindling/bootstrap.py) |
| [local_code_first_development.md](./local_code_first_development.md) | Implemented | `kindling new` scaffold templates + unit/component/integration test tiers |
| [pre_post_transform_analysis.md](./pre_post_transform_analysis.md) | Superseded | Signals framework provides the hook points; document itself notes "FULLY CONGRUENT" with signal_dag_streaming |
| [secret_provider_service.md](./secret_provider_service.md) | Implemented | [platform_provider.py](../../../packages/kindling/platform_provider.py) — `SecretProvider`, `PlatformServiceSecretProvider` |
| [signal_dag_streaming_evaluation.md](./signal_dag_streaming_evaluation.md) | Historical | Evaluation doc for the proposal below; both superseded by shipped code |
| [signal_dag_streaming_meta_evaluation.md](./signal_dag_streaming_meta_evaluation.md) | Historical | Meta-review of the evaluation above; no ongoing value |
| [signal_dag_streaming_proposal.md](./signal_dag_streaming_proposal.md) | Implemented | Signals, DAG execution, and streaming orchestrator all shipped — see individual files under [packages/kindling/](../../../packages/kindling/) |
| [simplified_bootstrap.md](./simplified_bootstrap.md) | Implemented | v0.9.0/v0.9.1 refactor (single wheel + platform extras + entry-point loader + lazy platform imports) |
| [single_notebook_bootstrap.md](./single_notebook_bootstrap.md) | Rejected | Evaluation concluded wheel distribution remains the right approach; single-notebook embedding impractical |

Still-relevant proposals live one level up in [docs/proposals/](..).
