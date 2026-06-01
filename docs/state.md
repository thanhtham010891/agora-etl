# State

_When to read this: you need a lightweight persisted key-value layer for checkpoints, caches, dedup markers, or your own runtime state._

Agora's state layer is the shared storage abstraction behind checkpoints, schema
stores, HTTP response caches, and other lightweight runtime data.

You can also use it directly in your own pipelines when you need a small amount
of persisted state without introducing a full external database abstraction.

## The main pieces

| Tool | What it does |
|---|---|
| `StateBackend` | Low-level key-value backend contract |
| `MemoryBackend` | In-process backend for tests and one-off runs |
| `SQLiteBackend` | Local persistent backend for single-process deployments |
| `TTLKeyValueStore` | Namespaced key-value store with optional TTL |
| `MembershipKeyStore` | Namespaced exact-membership store with optional TTL |
| `state_backend_registry` | Registry for built-in and plugin backends |

## Choosing a backend

| Backend | When to use |
|---|---|
| `MemoryBackend` | Tests, local demos, temporary state that can disappear on exit |
| `SQLiteBackend` | Local persistence, single-process workers, simple deployments |

Third-party backends can register themselves through
`agora.state.backends`. The official plugin bundle adds Redis-backed options for
multi-process or shared deployments.

## Using a backend directly

Backends store JSON-like values:

- dicts
- lists
- strings
- numbers
- booleans
- `None`

The low-level API is synchronous:

```python
from agora import SQLiteBackend

backend = SQLiteBackend(".agora_state.db")

backend.set("last_run", {"count": 10})
value = backend.get("last_run")

print(value.value if value is not None else None)
backend.close()
```

`get()` returns a `StoredValue`, which contains:

- `value`: the stored payload
- `expires_at`: the absolute expiration timestamp, if one was set

## Using TTLKeyValueStore

`TTLKeyValueStore` is the easiest way to build a small namespaced cache or
scratch store on top of a backend.

```python
from agora import SQLiteBackend, TTLKeyValueStore

store = TTLKeyValueStore(
    backend=SQLiteBackend(".agora_state.db"),
    namespace="enrichment_cache",
    default_ttl_s=3600,
)

store.set("user:42", {"plan": "pro"})
print(store.get("user:42"))
store.close()
```

The namespace becomes a key prefix internally, so `user:42` in the example
above is stored under `enrichment_cache:user:42`.

Use it when you need:

- a lightweight cache for external lookups
- small persisted flags or counters
- a shared store for schema or middleware side data

## Using MembershipKeyStore

`MembershipKeyStore` is a specialized exact-membership helper. It is useful when
you only care whether a key has been seen before.

```python
from agora import MembershipKeyStore, SQLiteBackend

seen = MembershipKeyStore(
    backend=SQLiteBackend(".agora_state.db"),
    namespace="seen_orders",
    default_ttl_s=86400,
)

if seen.mark_if_new("order-123"):
    print("first time")
else:
    print("already processed")

seen.close()
```

Common uses:

- idempotency markers
- exact dedup support
- small lease or ownership flags

`mark_if_new()` uses the backend's atomic `set_if_absent()` operation when the
backend provides it, so it is the safest method when more than one worker may
race to mark the same key.

## TTL behavior

Both helper stores support TTL:

```python
from agora import MemoryBackend, TTLKeyValueStore

store = TTLKeyValueStore(
    backend=MemoryBackend(),
    namespace="tokens",
)

store.set("access", "abc123", ttl_s=300)
```

A few practical notes:

- `default_ttl_s` applies when you do not pass `ttl_s=...` per write
- `ttl_s=None` means the value does not expire
- expiry is enforced lazily by the backend on reads and related operations

Because expiry is lazy, helper methods like `count()` are best treated as
operational approximations, not strict live cardinality guarantees.

## Backend registry

`state_backend_registry` lets config-driven code create a backend by name.

```python
from agora import state_backend_registry

backend = state_backend_registry.create("sqlite", path=".agora_state.db")
```

Built-in keys are:

- `memory`
- `sqlite`

Plugin packages can register more.

## Relationship to other Agora features

You normally do not need to wire state manually for these features, but they are
built on the same abstraction:

- checkpoint stores
- schema stores
- HTTP response caching
- AI cache backends that wrap state

That shared model is useful because you can standardize on one backend type
across several runtime features.

## Operational notes

- `SQLiteBackend` creates parent directories automatically and enables WAL mode.
- Backends are process-local objects. Share them intentionally if you want
  multiple stores to use the same underlying file or connection.
- Calling `close()` on `TTLKeyValueStore` or `MembershipKeyStore` closes the
  underlying backend too.
- Core state backends are synchronous. If you use them inside async-heavy code,
  keep values small and operations quick.
