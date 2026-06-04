# JsonLinesSink

_Use this when: the output should be append-friendly, easy to inspect, and tool-friendly._

`JsonLinesSink` writes one JSON object per line to a `.jsonl` file.

## Good fits

- archival outputs
- development exports
- downstream tools that already read JSONL

## Characteristics

- straightforward text output
- buffered flush behavior
- prefers `orjson` when available

## Example

```python
from agora.sinks.file.jsonlines import JsonLinesSink

sink = JsonLinesSink(
    path="output/records.jsonl",
    serializer=lambda record: record,
    flush_every=100,
)
```

## When not to use it

If the output is large and analytical, `ParquetSink` is often a better long
term choice.

