# HTTPSource

_Use this when: the upstream system is an HTTP API and a custom source should handle paging, rate limits, retries, or circuit breaking._

`HTTPSource` is an abstract base for polling-style HTTP extractors. It is not a
ready-made source by itself; it is the base class to subclass.

## Good fits

- paginated REST APIs
- cursor-based polling jobs
- APIs that need retry, backoff, or request throttling

## Characteristics

- subclass-based, not directly instantiated
- built-in rate limiting and retry helpers
- supports caching and circuit breaking
- checkpointing is custom per subclass

## Example

```python
from agora.sources.http.http import CircuitBreakerConfig, HTTPSource, StopFetching


class OrdersSource(HTTPSource[dict]):
    source_name = "orders_api"

    def __init__(self) -> None:
        super().__init__(
            base_url="https://api.example.com",
            requests_per_second=5.0,
            max_retries=3,
            cache_ttl_seconds=3600,
            circuit_breaker=CircuitBreakerConfig(failure_threshold=5),
        )
        self._page = 1

    async def fetch_batch(self):
        resp = await self.get("/orders", params={"page": self._page})
        items = resp.json()["items"]
        if not items:
            raise StopFetching
        for item in items:
            yield item
        self._page += 1
```

## Resume support

`HTTPSource` does not define a built-in checkpoint contract. If the extractor
must resume after restart, implement:

- `supports_checkpoint = True`
- `current_checkpoint()`
- `prepare_resume()`

That usually means storing a page token, cursor, watermark, or last seen ID.

