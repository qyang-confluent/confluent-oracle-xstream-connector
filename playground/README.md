# Oracle XStream CDC Source – Confluent Cloud Playground

Test the Confluent **Oracle XStream CDC Source connector** end to end:

```
Oracle (XStream outbound server) ──► Connect (standalone, Docker) ──► Confluent Cloud (Kafka + Schema Registry)
```

Terraform creates the Confluent Cloud side and **generates** the Connect worker and connector configs. A Docker image bundles Connect, the connector, and Oracle Instant Client. Two scripts start and stop the container.

## Layout

| Path | Purpose |
|---|---|
| `terraform/` | Confluent Cloud resources and the generated configs |
| `terraform/deploy.sh` | Loads the Cloud API key from the vault and runs `terraform apply` |
| `terraform/terraform.tfvars` | Your inputs (Oracle host, tables, etc.). Git-ignored |
| `terraform/generated/` | Output: `cp-local-worker.config`, `oracle-xstream-rac.config`. Git-ignored, contains secrets |
| `Dockerfile` | `cp-server-connect:7.9.2` + XStream connector + Oracle Instant Client 19c |
| `asset/` | Instant Client zips (x64 19.26, arm64 19.32), used at image build |
| `start-connector.sh` / `stop-connector.sh` | Build the image and run or remove the container |

## Prerequisites

- Docker. The image is built for the host architecture (arm64 on Apple Silicon, amd64 otherwise).
- **Oracle Instant Client (Basic) 19c zip**, downloaded from [Oracle's Instant Client page](https://www.oracle.com/database/technologies/instant-client/downloads.html) and placed in `asset/` (git-ignored, do not check it in). The image build needs the zip that matches your host architecture, with the exact file name below:
  - Linux x86-64 (amd64): `instantclient-basic-linux.x64-19.26.0.0.0dbru.zip`
  - Linux ARM64 (Apple Silicon): `instantclient-basic-linux.arm64-19.32.0.0.0dbru.zip`

  Using a different version means updating `IC_ZIP` and `IC_DIR` in `start-connector.sh` and the `Dockerfile`. It supplies `ojdbc8.jar` and `xstreams.jar`, which the connector needs.
- Terraform and a Confluent Cloud **Cloud API key/secret**. `deploy.sh` expects them in `~/vault/ccloud/dsp-cc.env`. That file must export `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET`.
- A Confluent Cloud environment (default name `Qi`) with **Stream Governance / Schema Registry already enabled**.
- An Oracle database, reachable from your machine, that is already set up for XStream:
  - the `CFLTUSER` connector user;
  - a running XStream **outbound server** (default `XOUT`) that captures your tables;
  - the tables you want to capture, supplemental logging, etc.

  Follow `../oracle-downstream-setup.md` and the other docs in the parent folder. For downstream capture, point at the downstream database (see step 1).

## Steps

### 1. Configure inputs

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # skip if it already exists
```

Edit `terraform.tfvars`:

```hcl
environment_name   = "Qi"
table_include_list = "HR.EMPLOYEES"        # SCHEMA.TABLE, comma-separated

oracle = {
  hostname        = "<oracle-host-or-scan>"
  user            = "CFLTUSER"
  password        = "<password>"
  dbname          = "ORCL"
  service_name    = "<service name>"        # `$` must be written as `\\$`
  out_server_name = "XOUT"
  # downstream = { hostname = "...", port = 1521, dbname = "STRMDB1", service_name = "..." }
}
```

Other variables in `variables.tf` that you may want to change:

| Variable | Default | Meaning |
|---|---|---|
| `cluster_name` | `poc` | New Dedicated cluster name |
| `cloud` / `region` / `cku` | `AWS` / `us-east-1` / `1` | Cluster placement and size |
| `connector_name` | `rac-xstream-cc-xxx` | Connector name |
| `topic_prefix` | `ORCL` | Prefix for data topics (`<prefix>.<SCHEMA>.<TABLE>`) |
| `snapshot_mode` | `no_data` | Snapshot behavior (`initial` loads existing rows) |
| `schema_history_topic` | `rac-orcl-schema-changes.cflt` | Schema history topic |
| `confluent_license` | empty | Empty means the 30-day trial |

Setting `oracle.downstream` adds the `downstream.database.*` settings to the connector config (downstream-capture topology).

### 2. Create Confluent Cloud resources and generate configs

```bash
./deploy.sh        # or: source ~/vault/ccloud/dsp-cc.env && terraform apply
```

`deploy.sh` has a hard-coded `cd` to this repo path. Adjust it if you moved the repo.

This creates:

- a Dedicated Kafka cluster
- a service account with `CloudClusterAdmin` on the cluster
- a Kafka API key and a Schema Registry API key
- the `confluent.topic` topic (`oracle-xstream-connector`)

It also writes the two config files into `terraform/generated/`. Check the outputs (`bootstrap_servers`, `schema_registry_url`, `generated_files`) with `terraform output`.

> A Dedicated cluster takes a while to provision and costs money while it exists. Run `terraform destroy` when you finish (see Cleanup).

### 3. Start the connector

From `playground/`:

```bash
./start-connector.sh              # builds xstream-connect:<arch> on first run
./start-connector.sh --rebuild    # force an image rebuild
```

The script mounts `terraform/generated/` read-only into the container, rewrites `plugin.path` for the container, and runs `connect-standalone` with the worker and connector configs.

Optional env overrides: `OUT_SERVER` (override `database.out.server.name`), `CONFIG_DIR`, `CONTAINER` (default `xstream-connect`), `PORT` (default `8083`).

```bash
OUT_SERVER=XOUT_DS ./start-connector.sh    # example: use a different outbound server
```

### 4. Verify

```bash
docker logs -f xstream-connect                         # watch startup; look for the XStream attach
curl -s localhost:8083/connectors                      # connector name
curl -s localhost:8083/connectors/<name>/status | jq   # connector and task should be RUNNING
```

Generate changes in Oracle (as a user who owns the captured table, then `COMMIT`):

```sql
INSERT INTO HR.EMPLOYEES (...) VALUES (...);
UPDATE HR.EMPLOYEES SET ... WHERE ...;
DELETE FROM HR.EMPLOYEES WHERE ...;
COMMIT;
```

Then check Confluent Cloud (UI, or CLI):

- topic `ORCL.HR.EMPLOYEES` receives one Avro event per change (`before`/`after`/`op`)
- the schema history topic exists
- the key and value schemas are registered in Schema Registry

```bash
confluent kafka topic consume ORCL.HR.EMPLOYEES --from-beginning \
  --value-format avro --cluster <lkc-id> --environment <env-id>
```

With `snapshot_mode = "initial"`, existing rows are also emitted on first start. With the default `no_data`, only schema is captured and you will only see new changes.

### 5. Re-test

| Goal | Command |
|---|---|
| Restart from a **fresh offset** (re-snapshot or start over) | `./stop-connector.sh && ./start-connector.sh` |
| Stop but **keep offsets** (resume where it left off) | `./stop-connector.sh --keep`, then `docker start xstream-connect` |
| Change connector settings | Edit `variables.tf` / `terraform.tfvars`, `terraform apply`, then `./stop-connector.sh && ./start-connector.sh` |
| Quick one-off tweak | Edit `terraform/generated/oracle-xstream-rac.config`, then restart. Terraform overwrites it on the next apply |

Standalone offsets live **inside the container** (`/tmp/connect.offsets`). Removing the container resets them. Re-running a snapshot on a topic that already has data may produce duplicates. Delete the topics and schema history topic first for a clean test.

## Troubleshooting

| Symptom | Check |
|---|---|
| `Missing .../cp-local-worker.config` | Run `terraform apply` first. The files come from `terraform/generated/` |
| `ORA-` / connection errors in logs | Hostname, port, `service_name` escaping (`\\$`), and that the container can reach the DB (VPN, firewall, SCAN) |
| `libnsl` / `libaio` / `UnsatisfiedLinkError` | Image build problem. Run `./start-connector.sh --rebuild` and check that the Instant Client zip matches the architecture |
| Connector stays `UNASSIGNED` or fails at start | Outbound server `XOUT` is missing or disabled, or the user is not its connect user |
| Auth errors against Kafka or Schema Registry | API keys in `generated/` came from the latest apply. Re-run `terraform apply` and restart |
| Schema history or `confluent.topic` errors | Both need the client SASL settings. Terraform already sets them with the right prefixes |
| License error | Set `confluent_license` or stay within the 30-day trial |

## Cleanup

```bash
./stop-connector.sh
cd terraform && source ~/vault/ccloud/dsp-cc.env && terraform destroy
```

## Security notes

- `terraform/generated/*`, `terraform.tfvars`, `terraform.tfstate*` and `env.sh` hold credentials (DB password, API keys, license). They are git-ignored. **Do not commit, share or paste them.**
- `asset/` (about 160 MB of Instant Client zips) is git-ignored and must not be checked in. Download the Oracle Instant Client Basic zips yourself and place them there (file names are in `start-connector.sh`).
- The service account has `CloudClusterAdmin`. That is fine for a PoC but too broad for anything else.
