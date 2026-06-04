# WebhookSink

_Use this when: the destination is an HTTP endpoint instead of a file, table, or message bus._

`WebhookSink` POSTs records to an HTTP endpoint. It can send records one by one
or buffer them into JSON arrays.

## Good fits

- downstream APIs
- Slack or Discord style automation hooks
- low-volume integrations where HTTP is the natural boundary

## Characteristics

- uses `httpx`
- optional batch mode
- retries on `429` and `5xx`
- flushes buffered payloads on close

## Example

```python
from agora.sinks.http.webhook import WebhookSink

sink = WebhookSink(
    url="https://api.example.com/ingest",
    headers={"Authorization": "Bearer token"},
    batch_mode=True,
    flush_every=50,
)
```

## When not to use it

If throughput and replay discipline are central, a broker or database sink is
usually a better system boundary than a webhook.

