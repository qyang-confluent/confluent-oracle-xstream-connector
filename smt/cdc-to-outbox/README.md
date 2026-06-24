# cdc-to-outbox-smt

A Kafka Connect [Single Message Transform (SMT)](https://kafka.apache.org/documentation/#connect_transforms)
that unwraps a CDC change-event envelope (e.g. a Debezium record with `before` / `after` / `op` /
`ts_ms`) and reshapes it into an **outbox-pattern** message.

## Output shape

| Field           | Type     | Description                                                        |
|-----------------|----------|--------------------------------------------------------------------|
| `id`            | string   | By default, the original record key joined with the captured table name, e.g. `101.EMPLOYEES` (composite keys join their columns with `_`; falls back to just the table name when the record has no key). If `id.field` is set, the value of that state field is used instead. |
| `aggregatetype` | string   | Static `aggregate.type`, or the captured table name if not set.    |
| `type`          | string   | Configured `event.type.field`, or the change `op` (`c`/`u`/`d`/`r`).|
| `payload`       | string   | The `after` state (or `before` for deletes), serialized as a schemaless JSON string. |
| `timestamp`     | int64    | The envelope `ts_ms` (omitted when `timestamp.field` is empty).    |

The outbox message always carries the single static schema
`io.confluent.connect.smt.Outbox` (the source table's columns do not leak into
it). The `payload` is produced with Kafka's `JsonConverter` using
`schemas.enable=false`, so it is plain JSON without a schema envelope.

Works with both schema-based records (`Struct`) and schemaless records (`Map`).
Tombstones (null value) are passed through unchanged. For deletes, the `before` state is used.

## Configuration

| Property            | Default | Description                                                        |
|---------------------|---------|--------------------------------------------------------------------|
| `id.field`          | `""`    | State field whose value becomes the outbox `id`. Empty → use the record key joined with the captured table name. |
| `aggregate.type`    | `""`    | Static `aggregatetype` value. Empty → use the captured table name (`source.table`, else the last topic segment). |
| `event.type.field`  | `""`    | Field whose value becomes `type`. Empty → use the `op` field.      |
| `op.field`          | `op`    | Name of the operation field in the envelope.                       |
| `before.field`      | `before`| Name of the before-state field.                                    |
| `after.field`       | `after` | Name of the after-state field.                                     |
| `timestamp.field`   | `ts_ms` | Name of the timestamp field. Empty → omit `timestamp`.             |

## Build

```bash
mvn clean package
```

This produces `target/cdc-to-outbox-smt-1.0.0-SNAPSHOT-jar-with-dependencies.jar`.

## Install

Copy the jar into a directory on the Connect worker's `plugin.path`:

```bash
cp target/cdc-to-outbox-smt-*-jar-with-dependencies.jar \
   /usr/share/java/cdc-to-outbox-smt/
```

Restart the Connect worker.

## Use

```json
{
  "transforms": "outbox",
  "transforms.outbox.type": "io.confluent.connect.smt.CdcToOutbox",
  "transforms.outbox.aggregate.type": "customer"
}
```

The outbox message is emitted with a **null key** (the original record key is
folded into the `id` field).

## Samples

Worked examples with the Confluent Oracle XStream CDC Source connector live under
[`samples/`](samples/):

- [`samples/`](samples/README.md) — the `CdcToOutbox` SMT reshaping change events
  into outbox messages, with before/after record samples.
- [`samples/regexrouter/`](samples/regexrouter/README.md) — using the built-in
  `RegexRouter` SMT to rename the connector's topics (and how to chain it with
  `CdcToOutbox`).

## Test

```bash
mvn test
```
