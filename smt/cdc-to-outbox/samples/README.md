# Sample: CdcToOutbox with the Oracle XStream CDC Source connector

This sample runs the `CdcToOutbox` SMT on change events produced by the
[Confluent Oracle XStream CDC Source connector](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html),
turning each CDC change event for the `SAMPLE.EMPLOYEES` table into an
outbox-pattern message.

## Files

| File                          | Purpose                                                       |
|-------------------------------|---------------------------------------------------------------|
| `oracle-xstream-cdc-outbox.json` | Connector config with the SMT attached.                    |
| `sample-change-event.json`    | The change event the connector emits (the SMT's input).       |
| `sample-outbox-output.json`   | The record after the SMT runs (the SMT's output).             |

## How it fits together

The Oracle XStream CDC connector captures row changes from `SAMPLE.EMPLOYEES`
and writes a Debezium-style envelope (`before` / `after` / `op` / `ts_ms` /
`source`) to the topic `cflt.SAMPLE.EMPLOYEES` (topic name =
`{topic.prefix}.{schema}.{table}`).

The SMT then reshapes each value into an outbox message:

| Outbox field    | Source                                                        |
|-----------------|---------------------------------------------------------------|
| `id`            | the original record key joined with the captured table name — e.g. `101.EMPLOYEES` |
| `aggregatetype` | the captured table name — `EMPLOYEES` (default, from `source.table`) |
| `type`          | the change op — `c`, `u`, `d`, `r` (snapshot)                 |
| `payload`       | the `after` row (or `before` for deletes), as a schemaless JSON string |
| `timestamp`     | envelope `ts_ms`                                              |

Compare `sample-change-event.json` (input) with `sample-outbox-output.json`
(output) to see the transform applied to an UPDATE.

The SMT emits a single fixed schema named `io.confluent.connect.smt.Outbox` in
which `payload` is a `string` holding the row serialized as schemaless JSON
(via `JsonConverter` with `schemas.enable=false`). The source table's columns do
not appear in the outbox schema, so adding or changing columns never alters it.
With the Avro converter, that one Outbox schema is what gets registered in
Schema Registry. The outbox message is emitted with a null key (the original
primary key is folded into the `id` field).

> **Note:** `CdcToOutbox` already unwraps the envelope, so do **not** also add
> Debezium's `ExtractNewRecordState` SMT — that would strip the `before`/`after`
> structure this SMT needs.

## Prerequisites

1. Build and install the SMT jar (from the repo root):

   ```bash
   mvn clean package
   cp target/cdc-to-outbox-smt-*-jar-with-dependencies.jar \
      /usr/share/java/cdc-to-outbox-smt/
   ```

   Place it on a directory in the Connect worker's `plugin.path` and restart the
   worker. Confirm the transform is loaded:

   ```bash
   curl -s localhost:8083/connector-plugins | \
     grep -o 'io.confluent.connect.smt.CdcToOutbox'
   ```

2. A running Oracle database configured for XStream (CDB/PDB, an XStream
   outbound server, and a capture user). See the
   [connector prerequisites](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/getting-started.html).

3. A Connect worker with the Oracle XStream CDC connector and Schema Registry
   reachable at the URLs in the config.

## Deploy

Edit `oracle-xstream-cdc-outbox.json` to match your environment (`database.*`,
`topic.prefix`, `table.include.list`, registry URL), then create the connector.
By default the outbox `id` is the record key plus the table name; set
`transforms.outbox.id.field` to use a specific state column instead:

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @oracle-xstream-cdc-outbox.json \
  http://localhost:8083/connectors | jq
```

## Verify

Make a change in Oracle and read the resulting outbox message:

```sql
UPDATE SAMPLE.EMPLOYEES SET SALARY = 9500 WHERE EMPLOYEE_ID = 101;
COMMIT;
```

```bash
kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --property schema.registry.url=http://localhost:8081 \
  --topic cflt.SAMPLE.EMPLOYEES \
  --from-beginning
```

You should see a record shaped like `sample-outbox-output.json`.
