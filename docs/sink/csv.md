# CsvSink

_Use this when: the output must stay in a plain CSV format that other people or systems already expect._

`CsvSink` writes records into a CSV file through a row mapper.

## Good fits

- operational exports
- simple interoperability with spreadsheet tools
- downstream systems that only accept CSV

## Characteristics

- explicit field ordering through `fieldnames`
- buffered flushes
- keeps a file handle open across the sink lifecycle
- supports `write_arrow_batch()` for Arrow-lane pipelines
- can downgrade from Arrow-native CSV writing to Python-row writing when the
  batch is not CSV-safe

## Example

```python
from agora.sinks.file.csv import CsvSink

sink = CsvSink(
    path="output/records.csv",
    row_mapper=lambda record: {
        "id": record["id"],
        "name": record["name"],
        "score": record["score"],
    },
    fieldnames=["id", "name", "score"],
    flush_every=100,
)
```

## Practical note

Choose `CsvSink` when CSV compatibility matters more than compression,
schema-rich typing, or analytical throughput.

For Arrow pipelines, `CsvSink` first tries `pyarrow.csv.write_csv()` for each
incoming `RecordBatch`. If Arrow CSV rejects the batch, the sink falls back to
its normal row path automatically and records runtime counters for that
downgrade.

Common downgrade triggers:

- nested Arrow columns such as `struct`, `list`, or other non-flat values
- invalid UTF-8 payloads that cannot be rendered directly through the Arrow CSV writer

For predictable performance, export a whitelist of flat columns before the CSV
sink. In practice that usually means:

- flatten nested data in middleware before the sink
- project only the columns that should appear in the CSV
- treat `CsvSink` as a flat export target, not a general nested-data archive
