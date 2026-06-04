# AI Cache Helpers

_Use this when: AI middleware would otherwise repeat the same provider call across similar records or reruns._

Agora ships a few cache helpers for AI middleware so prompts do not always hit
the provider again.

## What the helpers do

- cache AI responses by prompt and provider parameters
- reduce latency and provider cost
- make repeated local runs more predictable

## Cache choices

| Cache | Good fit |
|---|---|
| `InMemoryLLMCache` | tests and short-lived local runs |
| `SQLiteLLMCache` | local persistence across reruns |
| `StateBackendLLMCache` | reusing an existing `StateBackend` choice |

## Example

```python
from agora.ai import build_llm_cache


cache = build_llm_cache({"type": "sqlite", "path": ".cache/llm.db"})
```

Then pass that cache into an AI middleware:

```python
pipeline.pipe(
    AIEnrichMiddleware(
        provider=my_provider,
        prompt_template="Summarize {name} as JSON",
        cache=cache,
    )
)
```

## Practical advice

- use in-memory cache only for one process lifetime
- use SQLite when running the same enrichment or extraction pipeline locally
  many times
- use state-backend cache when cache choice should follow the broader runtime
  state backend

Plugin packages can register additional cache backends through
`agora.ai.caches`.

