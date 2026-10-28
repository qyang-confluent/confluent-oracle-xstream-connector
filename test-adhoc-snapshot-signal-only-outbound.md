# Test: Ad-hoc Snapshot with a Signal-Table-Only Outbound Server

## Objective

Verify that the Confluent Oracle XStream CDC Source connector can run an **ad-hoc snapshot** of one table (`HR.EMPLOYEES`) when:

1. The XStream outbound server captures changes on the **connector signal table only**.
2. The connector starts in `snapshot.mode=no_data` with the whole `HR` schema in scope.
3. The snapshot is requested after startup by inserting a row into the signal table.

The interesting part is the combination. The outbound server only sees signal-table redo, but the snapshot reads `HR.EMPLOYEES` over JDBC. The test checks that the snapshot works without the outbound server capturing `HR` at all.

> **Naming.** The standard Oracle sample table is `HR.EMPLOYEES`. The test case calls it "hr.employee". This document uses `HR.EMPLOYEES`. Substitute your own table name if it differs.

## Test Environment

Replace the placeholders. Do not put real passwords in this file.

| Item | Value |
|---|---|
| Oracle version | 19c or later, ARCHIVELOG on, minimal supplemental logging on |
| Topology | Local capture (`XOUT1` on the source). For downstream capture use `XOUT1_DS` and run the outbound steps on the downstream DB (see `oracle-downstream-setup.md`) |
| PDB / DB name | `<PDB_NAME>` (for example `ORCLPDB1`) |
| Connector user | `CFLTUSER` |
| Signal table | `CFLTUSER.CFLT_SIGNALS` |
| Outbound server | `XOUT1` |
| Topic prefix | `ORCL` |
| Kafka | Confluent Cloud cluster, Schema Registry enabled |
| Connect | Standalone worker from `playground/` (see `playground/README.md`) |

## Prerequisites

- The connector user `CFLTUSER` exists and has the privileges from the XStream setup guides.
- `HR.EMPLOYEES` exists and contains data. Record the row count before you start.
- No existing outbound server named `XOUT1`, or you are willing to drop and recreate it.
- No stale connector offsets, schema history topic or `ORCL.*` topics from earlier runs (see Cleanup).

Record the baseline:

```sql
-- as a DBA user, connected to the PDB
SELECT COUNT(*) AS employees_before FROM hr.employees;
```

---

## Part 1 — Outbound Server Captures the Signal Table Only

### 1.1 Create the signal table

Run as `CFLTUSER` (or grant it to `CFLTUSER`). The connector requires exactly these three columns.

```sql
CREATE TABLE cfltuser.cflt_signals (
    id   VARCHAR2(42) PRIMARY KEY,
    type VARCHAR2(32) NOT NULL,
    data VARCHAR2(2048)
);
```

### 1.2 Enable supplemental logging on the signal table

Run as `SYS` or a DBA.

```sql
ALTER TABLE cfltuser.cflt_signals ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### 1.3 Create the outbound server for the signal table only

Specify `table_names` and leave `schema_names` as `NULL`. Do not pass `hr`.

```sql
DECLARE
    v_tables  DBMS_UTILITY.UNCL_ARRAY;
BEGIN
    v_tables(1) := 'cfltuser.cflt_signals';
    DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
        server_name => 'XOUT1',
        table_names => v_tables,
        schema_names => NULL
    );
END;
/
```

If your Oracle release rejects the array form, use the string form from `oracle-downstream-setup.md` (Step 9) with `v_tables := 'cfltuser.cflt_signals'` and `v_schemas := NULL`.

Assign the connector user:

```sql
BEGIN
    DBMS_XSTREAM_ADM.ALTER_OUTBOUND(
        server_name  => 'XOUT1',
        connect_user => 'cfltuser'
    );
END;
/
```

### 1.4 Verify the outbound server rules

```sql
SELECT server_name, capture_name, source_database, connect_user FROM dba_xstream_outbound;
SELECT capture_name, status FROM dba_capture;

-- Rules should reference only CFLTUSER.CFLT_SIGNALS
SELECT streams_name, table_owner, table_name, rule_type, streams_rule_type
FROM   dba_xstream_rules
WHERE  streams_name IN (SELECT capture_name FROM dba_capture);
```

**Expected:**

- `XOUT1` exists and `dba_capture.status` is `ENABLED` (start it with `DBMS_XSTREAM_ADM.START_OUTBOUND` if not).
- Every rule row shows `CFLTUSER` / `CFLT_SIGNALS`. There is **no** `HR` rule.

---

## Part 2 — Start the Connector in `no_data` Mode for the HR Schema

### 2.1 Connector configuration

Key settings. Take the rest (Kafka, SSL, schema history, converters) from `playground/terraform/generated/oracle-xstream-rac.config` or `examples/downstream-capture-config.json`.

```json
{
  "name": "xstream-adhoc-snapshot-test",
  "connector.class": "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector",
  "tasks.max": "1",

  "database.hostname": "<oracle-host>",
  "database.port": "1521",
  "database.user": "CFLTUSER",
  "database.password": "<password>",
  "database.dbname": "<DB_NAME>",
  "database.service.name": "<service-name>",
  "database.out.server.name": "XOUT1",

  "topic.prefix": "ORCL",

  "snapshot.mode": "no_data",

  "table.include.list": "HR\\..*,CFLTUSER.CFLT_SIGNALS",

  "signal.enabled.channels": "source",
  "signal.data.collection": "<PDB_NAME>.CFLTUSER.CFLT_SIGNALS"
}
```

Points to check in the config:

- `snapshot.mode` is `no_data`. The connector captures schema only and does not read table data at startup.
- `table.include.list` covers every `HR` table **and** the signal table. The `HR` entry is what makes the schema in scope for CDC and for the ad-hoc snapshot. The signal table must be included so the connector sees signal inserts.
- `signal.data.collection` is the fully qualified `<PDB>.<SCHEMA>.<TABLE>` name of the signal table.
- Remove or underscore-prefix any competing `snapshot.mode` key. Duplicate keys silently take the last value.

### 2.2 Start the connector with the playground scripts

The `playground/` scripts run the connector in a standalone Connect container against Confluent Cloud. Terraform generates the configs and `start-connector.sh` runs them. See `playground/README.md` for the prerequisites (Docker, the Instant Client zip in `playground/asset/`, a Cloud API key in `~/vault/ccloud/dsp-cc.env`).

**a. Set the inputs.** In `playground/terraform/terraform.tfvars`:

```hcl
environment_name   = "Qi"
table_include_list = "HR\\..*,CFLTUSER.CFLT_SIGNALS"   # whole HR schema + signal table
snapshot_mode      = "no_data"

oracle = {
  hostname        = "<oracle-host-or-scan>"
  user            = "CFLTUSER"
  password        = "<password>"
  dbname          = "<DB_NAME>"
  service_name    = "<service name>"      # write `$` as `\\$`
  out_server_name = "XOUT1"
}
```

`table_include_list` goes into the connector config as a comma-separated list of regexes. `HR\\..*` in HCL becomes the regex `HR\..*`.

**b. Generate the configs.**

```bash
cd playground/terraform
./deploy.sh        # sources the Cloud API key, runs terraform apply
```

The first apply creates the Confluent Cloud cluster, which takes a while. Later applies only rewrite `terraform/generated/`.

**c. Add the signal settings.** Put them in `extra_connector_config` in the same `terraform.tfvars`. The map is merged last into the generated connector config, so these keys survive every `terraform apply`. Values must be strings.

```hcl
extra_connector_config = {
  "signal.enabled.channels" = "source"
  "signal.data.collection"  = "<DB_NAME>.CFLTUSER.CFLT_SIGNALS"
}
```

Run `./deploy.sh` again, then confirm the generated config (without printing secrets):

```bash
jq '{"snapshot.mode", "table.include.list", "database.out.server.name", "signal.enabled.channels", "signal.data.collection"}' generated/oracle-xstream-rac.config
```

Do not print or commit the full generated file, because it holds secrets.

**d. Start.** Make sure Part 1 is done first, because the connector needs `XOUT1` to exist.

```bash
cd playground
./start-connector.sh              # first run builds the image
```

To point at a differently named outbound server without editing the config, use `OUT_SERVER=XOUT1_DS ./start-connector.sh`.

### 2.3 Verify startup

The connector name is the `connector_name` Terraform variable (default `rac-xstream-cc-xxx`), not the name in the sample JSON above. Use that name in the status call.

```bash
docker logs xstream-connect 2>&1 | grep -iE "snapshot|XOUT1|signal|ERROR|WARN" | tail -40

curl -s localhost:8083/connectors                          # shows the connector name
curl -s localhost:8083/connectors/<connector-name>/status | jq
```

**Expected:**

- Connector and task state are `RUNNING`.
- Logs show the snapshot mode as `no_data` and the connector attaching to outbound server `XOUT1`.
- The schema history topic receives DDL for the in-scope tables.
- Topics `ORCL.HR.*` hold **no** records. (`ORCL.HR.EMPLOYEES` may exist but be empty, depending on topic creation settings.)

The playground writes Avro, so consume with `--value-format avro`. Add `--cluster <lkc-id> --environment <env-id>` if they are not your CLI defaults (`terraform output` shows the cluster).

```bash
confluent kafka topic list | grep -i "ORCL"
confluent kafka topic consume ORCL.HR.EMPLOYEES --from-beginning --value-format avro --timeout 20   # expect 0 records
```

### 2.4 Confirm that HR changes are not streamed (control check)

Because the outbound server only captures the signal table, HR DML should not reach Kafka. This check documents that behavior and is not a defect.

```sql
-- as an HR-privileged user
UPDATE hr.employees SET salary = salary WHERE employee_id = 100;
COMMIT;
```

**Expected:** no new record on `ORCL.HR.EMPLOYEES`.

If a record does appear, the outbound server rules include `HR`. Return to step 1.4.

---

## Part 3 — Signal the Ad-hoc Snapshot for `HR.EMPLOYEES`

### 3.1 Insert the signal

Run as `CFLTUSER` after the connector is `RUNNING`.

```sql
INSERT INTO cfltuser.cflt_signals (id, type, data)
VALUES (
    'adhoc-snap-employees-001',
    'execute-snapshot',
    '{"data-collections": ["<PDB_NAME>.HR.EMPLOYEES"], "type": "blocking"}'
);
COMMIT;
```

Notes:

- `id` must be unique per signal. Use a new id for every re-run.
- `data-collections` uses `<PDB>.<SCHEMA>.<TABLE>`.
- `type` is `blocking`, matching what this repo already uses for ad-hoc signals. If your connector version also supports `incremental`, repeat the test with that type as a variation.

### 3.2 Verify the signal was picked up

```bash
docker logs xstream-connect 2>&1 | grep -iE "signal|execute-snapshot|snapshot" | tail -40
```

**Expected log sequence:**

1. The signal row is read from the signal table (the change arrives through `XOUT1`, which proves part 1 works).
2. The connector starts a snapshot for `HR.EMPLOYEES` only.
3. The snapshot completes with a success message.
4. The connector returns to streaming and stays `RUNNING`.

### 3.3 Verify the data in Kafka

```bash
confluent kafka topic consume ORCL.HR.EMPLOYEES --from-beginning --value-format avro --timeout 30 | wc -l
```

```sql
SELECT COUNT(*) AS employees_after FROM hr.employees;
```

---

## Pass / Fail Criteria

| # | Check | Expected | Result |
|---|---|---|---|
| 1 | `dba_xstream_rules` for `XOUT1` | Only `CFLTUSER.CFLT_SIGNALS` | |
| 2 | Connector and task status after start | `RUNNING` | |
| 3 | `ORCL.HR.*` records before the signal | 0 | |
| 4 | HR DML before the signal (step 2.4) | No record on `ORCL.HR.EMPLOYEES` | |
| 5 | Signal row consumed | Log shows `execute-snapshot` received | |
| 6 | Record count on `ORCL.HR.EMPLOYEES` | Equals `employees_before` | |
| 7 | Snapshot record `op` field | `r` (read) | |
| 8 | Other HR tables (for example `ORCL.HR.DEPARTMENTS`) | Still 0 records | |
| 9 | Connector state after snapshot | `RUNNING`, no `ERROR` in logs | |

Inspect one record to check item 7:

```bash
confluent kafka topic consume ORCL.HR.EMPLOYEES --from-beginning --value-format avro --timeout 20 | head -1
```

Look for `"op":"r"` and a populated `after` struct.

## Expected Behavior and Risks

- **No streaming CDC for HR.** The outbound server filters to the signal table. HR inserts, updates and deletes after the snapshot will not appear in Kafka. If the real goal is continuous CDC for the whole `HR` schema, add the schema to the outbound server (`DBMS_XSTREAM_ADM.ADD_SCHEMA_RULES`) and repeat part 2.4 expecting events.
- **Snapshot is independent of XStream.** The snapshot reads the table over JDBC. If it works while the outbound server has no HR rules, that is the confirmed behavior. If your connector version refuses to snapshot a table the outbound server does not capture, record the exact error and treat it as a finding.
- **Signal discovery depends on `XOUT1`.** If the signal row is not seen, check the supplemental logging on the signal table and that the capture process is `ENABLED`.
- **Duplicates.** Re-running the signal on a topic that already has data produces duplicate records.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Signal row ignored, no log entry | Signal table not in `table.include.list`, wrong `signal.data.collection`, missing supplemental logging, or capture not `ENABLED` |
| Snapshot not started, "unknown data collection" | `data-collections` value does not match `<PDB>.<SCHEMA>.<TABLE>` casing, or the table is outside `table.include.list` |
| `ORA-` error on connect | `connect_user` not set to `cfltuser` on `XOUT1`, or missing privileges |
| Data appears at startup | `snapshot.mode` is still `initial`; check for a duplicate key in the config |

## Cleanup

```sql
DELETE FROM cfltuser.cflt_signals WHERE id = 'adhoc-snap-employees-001';
COMMIT;
```

```bash
cd playground
./stop-connector.sh          # removes the container and standalone offsets (fresh start next time)
./stop-connector.sh --keep   # alternative: stop only, `docker start xstream-connect` resumes with offsets
```

To run the test again from scratch: `./stop-connector.sh`, delete the `ORCL.HR.*` topics and the schema history topic, use a new signal `id`, then `./start-connector.sh`. When you finish with Confluent Cloud, run `terraform destroy` in `playground/terraform` to stop the Dedicated cluster costs.

Delete the `ORCL.HR.*` topics and the schema history topic before the next run. To remove the outbound server:

```sql
BEGIN
    DBMS_XSTREAM_ADM.DROP_OUTBOUND(server_name => 'XOUT1');
END;
/
```
