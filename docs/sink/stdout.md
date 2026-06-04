# StdoutSink

_Use this when: the goal is to see what the pipeline produces before wiring a real destination._

`StdoutSink` prints each record to standard output.

## Good fits

- local debugging
- first-pass pipeline verification
- dry-run style development

## Characteristics

- zero setup
- human-readable output
- not a durable destination

## Example

```python
from agora import IterableSource, Pipeline
from agora.sinks.io.stdout import StdoutSink

summary = await (
    Pipeline(IterableSource([{"id": 1}, {"id": 2}]))
    .build(StdoutSink(prefix="[record] "))
    .run()
)
```

