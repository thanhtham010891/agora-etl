# Upgrading and Compatibility

_When to read this: before upgrading `agora-etl`, the official plugin bundle,
or a checkpointed deployment._

This page is the release policy for supported upgrades. It does not weaken the
runtime guarantees in [Runtime Guarantees](runtime-guarantees.md): a successful
package upgrade is not permission to reuse a checkpoint cursor against a
different input.

## Compatibility contract

| Surface | Current policy | Operator action |
|---|---|---|
| Core public APIs | Stable public contracts preserve semantics through the current major line. | Import from the documented `agora` or `agora.core.<domain>` facade. |
| Root-facade convenience exports | Selected exports are soft-deprecated, retained throughout `0.4.x`, with removal target `0.5.0`. | Replace root imports using the `DeprecationWarning` target before the removal release. |
| Plugin discovery | A plugin manifest with a mismatched `AGORA_PLUGIN_MANIFEST_VERSION` is excluded and reported in diagnostics. | Run `agora plugins list --json`; upgrade or rebuild the incompatible plugin. |
| Checkpoints | Resume is permitted only when the source's identity contract accepts the saved checkpoint. | Treat identity mismatch as a recovery decision, not a package-install error. |
| Backend semantics | Kafka, Redis, PostgreSQL, S3, and BigQuery behavior remains plugin-owned. | Run the relevant documented plugin gate before claiming production readiness. |

The root facade intentionally keeps builders such as `Pipeline` and
`DeliveryConfig`. Extension contracts such as `BaseSource`, `BaseSink`,
`CheckpointStore`, and `Writer` belong to their named `agora.core.*` domains.
The old imports still resolve in `0.4.x`, emit `DeprecationWarning`, and name
the replacement. They are not silently removed in a patch release.

## Safe upgrade procedure

1. Pin a compatible core and plugin bundle in the same deployment artifact.
   Do not mix an unverified plugin wheel with a newly upgraded core.
2. Back up the checkpoint store and record its namespace/key before changing
   packages or configuration.
3. In staging, run `agora plugins list --json` and resolve every
   `compatible=false`, load error, or entry-point collision before promotion.
4. Run `pipeline.explain()` and `agora doctor --config ...` with the deployed
   configuration. Review delivery-policy mismatches and required acceleration
   failures before opening sources or sinks.
5. For every checkpointed source, perform one resume test against a copied
   checkpoint store and the same immutable input/version. Run the matching
   backend integration gate for any flagship plugin path.
6. Promote only after the checkpoint, delivery, and plugin diagnostics are
   clean. Retain the old deployment artifact and checkpoint backup for the
   rollback window.

## Checkpoint migration after source identity support

Older checkpoints have no `source_identity`. New identity-aware sources reject
them by default before consuming records. This is intentional: preserving an
old cursor without proving the input is identical can silently skip data.

Choose exactly one migration path per source and document it in the deployment
runbook:

| Decision | Use when | Consequence |
|---|---|---|
| Keep `fail_closed` | The old cursor or input provenance is uncertain. | The deployment stops before reads; inspect, reset, or restore a known-safe checkpoint. |
| Use `reset` once | Re-reading the current immutable input is safe and duplicates are handled downstream. | Starts at the beginning and writes a new identity-bound checkpoint after success. |
| Use `allow` once | The operator can prove the old cursor remains valid for the identical ordered input. | Can skip or duplicate data if that proof is wrong; remove the override after migration. |

Do not use `allow` merely to make an upgrade start. `checkpoint_failure_policy`
does not override this source-level fail-closed decision.

## Rollback

Restore the previous package artifact and the checkpoint-store backup as one
operation. Do not assume an older runtime can safely interpret every checkpoint
written by a newer runtime, especially after source identity, source ordering,
or backend configuration changes. If the checkpoint is not known-safe for the
rollback artifact, reset/replay according to the source and sink delivery
contract instead of forcing a cursor.

## Evidence required for a release

- Public facade/deprecation and plugin manifest compatibility tests pass.
- The relevant backend release gate passes for each support claim.
- Upgrade staging records `agora plugins list --json`, `agora doctor`, and a
  resume result for checkpointed pipelines.
- Any migration that chooses `reset` or `allow` has an explicit duplicate and
  rollback plan.

This keeps compatibility a tested operational contract rather than a promise
hidden in package version ranges.
