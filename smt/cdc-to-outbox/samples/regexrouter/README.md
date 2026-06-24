# Sample: RegexRouter with the Oracle XStream CDC Source connector

This example uses Kafka Connect's built-in
[`RegexRouter`](https://docs.confluent.io/platform/current/connect/transforms/regexrouter.html)
SMT (`org.apache.kafka.connect.transforms.RegexRouter`) to rename the topics
produced by the
[Oracle XStream CDC Source connector](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html).

No custom code is involved — `RegexRouter` ships with Apache Kafka Connect.

## What it does

The connector writes change events to `{topic.prefix}.{schema}.{table}`. For the
two captured tables that is:

| Source topic (from connector) | Routed topic (after RegexRouter) |
|-------------------------------|----------------------------------|
| `cflt.SAMPLE.EMPLOYEES`       | `cdc.EMPLOYEES`                  |
| `cflt.SAMPLE.DEPARTMENTS`     | `cdc.DEPARTMENTS`               |

`RegexRouter` only rewrites the **topic name** — the record key, value and
schema are untouched.

The rule:

```properties
transforms=route
transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter
transforms.route.regex=cflt\.SAMPLE\.(.+)
transforms.route.replacement=cdc.$1
```

`regex` is matched against the whole topic name; the capture group `(.+)` (the
table name) is substituted into `replacement` as `$1`.

> Note on backslashes: in the JSON config file the regex is written
> `"cflt\\.SAMPLE\\.(.+)"` because JSON requires the backslash to be escaped.
> The connector sees `cflt\.SAMPLE\.(.+)`.

## Useful variants

Collapse **any** `{prefix}.{schema}.{table}` name down to just the table:

```properties
transforms.route.regex=[^.]+\.[^.]+\.(.+)
transforms.route.replacement=$1
```

Route **all** captured tables into a single topic (e.g. for a fan-in consumer):

```properties
transforms.route.regex=.*
transforms.route.replacement=oracle.cdc.all
```

## Chaining with CdcToOutbox

`RegexRouter` (value-preserving, topic-only) composes cleanly with the
`CdcToOutbox` SMT in [`../`](../README.md). List both, routing last:

```properties
transforms=outbox,route
transforms.outbox.type=io.confluent.connect.smt.CdcToOutbox
transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter
transforms.route.regex=cflt\.SAMPLE\.(.+)
transforms.route.replacement=outbox.$1
```

## Deploy

Edit `oracle-xstream-regexrouter.json` for your environment, then:

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @oracle-xstream-regexrouter.json \
  http://localhost:8083/connectors | jq
```

## Verify

```bash
# The original prefixed topics should no longer receive records; the routed ones do.
kafka-topics --bootstrap-server localhost:9092 --list | grep -E '^cdc\.'

kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --property schema.registry.url=http://localhost:8081 \
  --topic cdc.EMPLOYEES \
  --from-beginning
```

After an `UPDATE` on `SAMPLE.EMPLOYEES`, the change event appears on
`cdc.EMPLOYEES` instead of `cflt.SAMPLE.EMPLOYEES`.
