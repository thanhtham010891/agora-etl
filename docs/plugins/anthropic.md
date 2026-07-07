# Anthropic

_When to read this: the pipeline needs Claude-style completion or structured
JSON output._

Anthropic support is part of the official first-party `agora-etl-plugins`
package through the `anthropic` extra.

The supported boundary is intentionally focused: Claude completions and
structured output, not embeddings.

In the official `agora-etl-plugins 0.4.x` line, Anthropic is an official AI
provider surface for completion-driven middleware. API keys, model choice,
data retention policy, and cost controls remain deployment responsibilities.

## Contract summary

- Package: `agora-etl-plugins[anthropic]`
- Registry key: `anthropic`
- Entry-point group: `agora.ai.providers`
- Import path: `agora_plugins.anthropic`
- Supported scope: completion and structured output

This page documents the official Anthropic path as it exists today: a focused
provider for completion-driven AI workflows that keeps its contract explicit.

## What it supports

- `complete()` for ordinary prompt/response usage
- structured JSON responses through Agora's `response_format` path
- completion-driven AI middlewares such as `AIEnrichMiddleware`,
  `AIExtractMiddleware`, `AIValidateMiddleware`, `AITranslateMiddleware`,
  `AIBatchMiddleware`, and `AIClassifyMiddleware` in LLM mode

## What it does not support

- native embeddings
- `AIClassifyMiddleware(use_embeddings=True)`
- `EmbeddingStore`

Anthropic does not provide native embedding models in this package's support
story. `AnthropicProvider` declares `supports_embeddings=False`, so
embedding-based workflows are rejected by Agora's embedding provider guard even
if legacy compatibility methods are present. For embedding-based workflows,
prefer an embedding-capable Agora provider package instead.

## Install

```bash
pip install "agora-etl-plugins[anthropic]"
```

Then import with:

```python
from agora_plugins.anthropic import AnthropicProvider
```

## Credentials

Set credentials with:

- `ANTHROPIC_API_KEY` in the environment
- `api_key=...` when constructing `AnthropicProvider`

## Production checklist

- Store API keys in the deployment secret manager, not in pipeline config files.
- Set explicit model names instead of relying on changing provider defaults.
- Use Agora AI governance and budget controls for unattended workloads.
- Treat prompts and completions as potentially sensitive operational data.
- Use completion and structured-output middleware only; choose an
  embedding-capable provider for embedding workflows.

In the current `agora-etl-plugins 0.4.x` line, the default Claude API model is
`claude-haiku-4-5-20251001`. The provider's built-in allowlist is intended to
track active production model ids on Anthropic-operated surfaces. If a team
needs to trial a newer id before the bundle is refreshed, it must opt in with
`allow_unknown_models=True`.

## End-to-end example

This is the recommended supported path: use Anthropic to normalize messy
catalog text into clean product fields, not for embeddings.

```python
from agora import Pipeline
from agora.middlewares.ai.extract import AIExtractMiddleware
from agora.sinks import StdoutSink
from agora.sources import IterableSource
from agora_plugins.anthropic import AnthropicProvider


source = IterableSource(
    [
        {
            "sku": "DRNK-8841",
            "raw_description": (
                "Cold brew concentrate, 1L bottle. Unsweetened. Dark roast blend. "
                "Keeps 14 days refrigerated after opening. Suitable for drink "
                "service counters or office pantry restock."
            ),
        }
    ]
)

provider = AnthropicProvider(model="claude-haiku-4-5-20251001")
middleware = AIExtractMiddleware(
    provider=provider,
    source_field="raw_description",
    extract={
        "pack_size_ml": "Pack size in milliliters as integer, null if missing",
        "sweetness": "One of unsweetened, low, medium, high, or null",
        "bean_type": "Coffee bean type as short string, null if missing",
        "storage_after_opening": "Short storage instruction string, null if missing",
        "target_channel": "Best-fit sales channel such as retail, cafe, office, horeca",
    },
)

summary = (
    Pipeline(source)
    .pipe(middleware)
    .build(StdoutSink())
    .run_sync()
)
```

## Support boundary

This provider is official, but its scope stays intentionally narrow:

- it supports completion-driven middleware flows and structured JSON output
- it is not an embedding-capable provider and declares
  `supports_embeddings=False`
- embedding-based features should use an embedding-capable provider instead

That keeps the Anthropic path honest while still making it part of the
official first-party plugin package.
