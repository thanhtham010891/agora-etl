# Source Of Truth Map

_When to read this: you need to know which document is authoritative for a claim, contract, support boundary, or package story._

Agora keeps READMEs intentionally shorter than the full documentation set.
When two pages mention the same topic, use the table below to decide which one
is canonical.

## Public truth map

| Topic | Canonical document | Supporting docs | Notes |
|---|---|---|---|
| Core package quickstart and install | `agora-etl` README | [Quickstart](guides/quickstart.md) | README is the shortest onboarding path for `agora-etl`. |
| Core runtime semantics and guarantees | [Runtime Guarantees](guides/runtime-guarantees.md) | [Architecture](architecture.md), [Failure Handling](guides/failure-handling.md), [Checkpointing](guides/checkpointing.md) | If wording differs anywhere else, the runtime guarantees page wins. |
| Upgrade, rollback, checkpoint migration, and API deprecation | [Upgrading and Compatibility](guides/upgrading.md) | [Checkpointing](guides/checkpointing.md), [Plugin Contract](plugins/contract.md), [Manifest Contract](plugins/manifest.md) | This page owns the release procedure; source-specific recovery rules still apply. |
| Core/package boundary | [docs/index.md](index.md) | `agora-etl` README, [Plugins](plugins/index.md) | Use this to answer “core or plugin?” questions. |
| Plugin ecosystem overview | [Plugins](plugins/index.md) | `agora-etl-plugins` README | The plugin landing page is canonical for family navigation. |
| Plugin maturity and support boundaries | [Plugin Production Readiness](plugins/production-readiness.md) | plugin family pages, `agora-etl-plugins` README | Release/support claims should map back here. |
| Plugin author contract | [Plugin Contract](plugins/contract.md) | [Developing Plugins](plugins/developing.md), [Manifest Contract](plugins/manifest.md) | If a plugin surface question is about contract shape, this page wins. |
| Family-specific capability and operator guidance | family pages under `docs/plugins/*.md` | `agora-etl-plugins` README | Redis/Kafka/PostgreSQL/BigQuery pages own backend-specific public detail. |
| CLI command behavior | [CLI](cli.md) | `agora-etl` README | Command shape and flags belong in the CLI reference. |
| Security reporting | package `SECURITY.md` files | README security sections | Follow the package policy, not an incidental doc mention. |
| Release history | package changelogs and docs change-log pages | release notes | Historical statements can describe old behavior; they do not override current canonical docs. |

## README policy

- `agora-etl` README is the quickstart and package boundary story for the core runtime.
- `agora-etl-plugins` README is the quickstart and install story for the official plugin bundle.
- READMEs should point to canonical docs for deep support boundaries, maturity claims, and backend runbooks instead of duplicating them.

## Maturity policy

- Family landing pages should begin with a compact maturity card.
- Cross-family support claims should be restated in [Plugin Production Readiness](plugins/production-readiness.md).
- Composed-flow helpers must not be described as default onboarding primitives.
