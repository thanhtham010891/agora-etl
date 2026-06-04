# DedupMiddleware

_Use this when: duplicate records should be stopped before they reach expensive or irreversible downstream work._

`DedupMiddleware` combines a key extractor with a store and, optionally, a
matching strategy.

## What it does

- computes a dedup key from each record
- checks whether that key has already been seen
- drops duplicates
- stores new keys when the record is accepted

## When it is a good fit

- records have a stable identity such as `id`, `slug`, or external key
- duplicate delivery is expected from the upstream source
- downstream writes should be reduced before they reach a sink

## Exact and fuzzy modes

- exact mode is the default and is the normal production choice
- fuzzy mode compares the new key against previously seen keys through a
  strategy such as `FuzzyMatchStrategy`

Fuzzy mode is more expensive and keeps an in-memory list of seen keys. It is
best for modest working sets, not large-scale semantic matching.

## Store failure behavior

`store_failure_policy` controls what happens if the backing dedup store fails:

- `fail_closed`: raise and let the runtime treat it as a middleware failure
- `fail_open`: log the store error and pass the record through

## Example: exact dedup

```python
from agora.middlewares.dedup import DedupMiddleware


pipeline.pipe(DedupMiddleware(key=lambda record: str(record["id"])))
```

## Example: fuzzy dedup

```python
from agora.middlewares.dedup import DedupMiddleware
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.middlewares.dedup.strategies.fuzzy import FuzzyMatchStrategy


pipeline.pipe(
    DedupMiddleware(
        key=lambda record: record["name"].lower(),
        store=InMemoryStore(),
        strategy=FuzzyMatchStrategy(threshold=0.85),
        max_fuzzy_keys=100_000,
    )
)
```

## When to choose something else

- use sink-side upsert logic when duplicates must still reach the destination
- use plugin-backed stores such as Redis for shared dedup across workers or pods

