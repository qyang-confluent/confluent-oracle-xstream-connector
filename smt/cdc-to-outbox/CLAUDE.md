# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

A Kafka Connect **Single Message Transform (SMT)** that unwraps a CDC change-event
envelope (Debezium-style: `before` / `after` / `op` / `ts_ms` / `source`) and
reshapes it into an **outbox-pattern** message: `id`, `aggregatetype`, `type`,
`payload`, `timestamp`. The `payload` is the row serialized as a **schemaless JSON
string** (via `JsonConverter` with `schemas.enable=false`), so the outbox message
always carries one static schema (`io.confluent.connect.smt.Outbox`) regardless of
the source table's columns.

Target: Kafka/Connect 3.7.x, Java 17, Maven.

## Layout

```
src/main/java/io/confluent/connect/smt/CdcToOutbox.java   # the SMT
src/test/java/io/confluent/connect/smt/CdcToOutboxTest.java
samples/                       # CdcToOutbox + Oracle XStream CDC example (config + before/after records)
samples/regexrouter/           # built-in RegexRouter example with the same connector
add-to-existing-repo.sh        # copy this project into a subdir of another repo and push
pom.xml
```

## Commands

```bash
mvn clean test          # run unit tests (JUnit 5)
mvn clean package       # build + produce the deployable jar
```

The deployable artifact is `target/cdc-to-outbox-smt-*-jar-with-dependencies.jar`;
copy it to a directory on the Connect worker's `plugin.path`.

## Architecture / behavior notes

- `CdcToOutbox<R extends ConnectRecord<R>>` operates on the record **value** and
  handles two input shapes:
  - schema-based (`Struct`) → `applyWithSchema`
  - schemaless (`Map`) → `applySchemaless`
- Tombstones (null value) are **passed through unchanged**.
- For deletes (`op=d`, `after` null) the `before` state is used as the payload.
- `aggregate.type` empty → falls back to the captured table name (envelope
  `source.table`, else the last dot-segment of the topic). `event.type.field`
  empty → uses the `op` value.
- `id`: by default the original record key joined with the captured table name
  (`<key>.<table>`; composite keys join with `_`). `id.field` overrides this with
  the value of a state field. The outbox record itself is emitted with a **null
  key** (the key is folded into `id`); tombstones keep their key and pass through.
- The output schema is fixed (built once in `configure()`); `payload` is
  `OPTIONAL_STRING`. Do not reintroduce per-payload schema derivation/caching —
  payload is now a JSON string, so the schema no longer varies.
- JSON serialization uses an instance `JsonConverter`; `connect-json` is a
  bundled (compile-scope) dependency so it is available under Connect's isolated
  plugin classloader. `connect-api` / `connect-transforms` are `provided`.

## Conventions

- Match the existing code style; keep the `Transformation` contract intact
  (`configure` / `apply` / `config` / `close`).
- Every behavior change should come with a unit test. Tests assert the exact JSON
  payload string for schema-based input; schemaless input uses substring checks
  because `HashMap` ordering is not deterministic.
- Keep `samples/` and the READMEs in sync with the SMT's output shape when it
  changes.

## Packaging

- The deployable `*-jar-with-dependencies.jar` is built by **maven-shade-plugin**
  (not assembly). Shade bundles only `compile`/`runtime` deps — `connect-json` +
  Jackson (~2.3M) — and excludes `provided` deps (`connect-api`/`kafka-clients`).
  The old assembly `jar-with-dependencies` descriptor wrongly bundled the entire
  provided tree (kafka-clients + native libs), bloating the jar to ~19M; do not
  switch back to it.
