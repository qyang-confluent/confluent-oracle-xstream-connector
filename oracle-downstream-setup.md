# Oracle Downstream Database Setup for XStream CDC Source Connector

This guide walks a DBA through configuring a dedicated **downstream capture** database for the Confluent Oracle XStream CDC Source connector. In this topology the XStream outbound server runs on a separate downstream database that receives redo logs from the source database, so mining activity does not compete with production workloads.

## Overview

```
Source DB (db1)          Downstream DB (strmdb1)
─────────────────        ──────────────────────────────────
Online redo logs  ──►    Standby redo logs → LogMiner
                                │
                         XStream Capture (CAP$_XOUT_DS_1)
                                │
                         XStream Outbound Server (XOUT_DS)
                                │
                     Confluent XStream Source Connector
```

The source database ships redo asynchronously to the downstream database using Oracle's log transport (RFS). LogMiner on the downstream database mines the incoming redo and feeds the XStream outbound server, which the connector reads.

## Prerequisites

- Both databases must run **Oracle 19c or later** with the Streams/XStream option licensed.
- The downstream database must be a full Oracle instance (not a physical standby) — it only receives redo from the source; it does not replicate all data changes.
- Network connectivity (SQL*Net) between source and downstream must be in place in both directions.
- The source database must already have **Supplemental Logging** and **ARCHIVELOG mode** enabled.
- Run all SQL as `SYS` (or a DBA-privileged account) unless otherwise noted.

---

## Step 1 — Enable Global Names on Both Databases

A unique global name is required for XStream's cross-database messaging. Run the following on **each database separately**.

```sql
-- Set a shared domain (both databases must use the same domain)
ALTER SYSTEM SET db_domain = 'myoracle.org' SCOPE = SPFILE;
ALTER SYSTEM SET global_names = TRUE;
SHUTDOWN ABORT;
STARTUP;
```

After restarting, assign each database its unique global name:

```sql
-- On the SOURCE database
ALTER DATABASE RENAME GLOBAL_NAME TO db1.myoracle.org;

-- On the DOWNSTREAM database
ALTER DATABASE RENAME GLOBAL_NAME TO strmdb1.myoracle.org;
```

Verify:

```sql
SELECT * FROM GLOBAL_NAME;
```

---

## Step 2 — Copy the Oracle Password File to the Downstream Database

The RFS process on the downstream database authenticates to the source using the `SYS` password. Both databases must share the same `SYS` password, or a dedicated redo transport user must be configured (see the troubleshooting section).

Copy the source password file to the downstream host:

```bash
# On the source host — locate the password file
ls $ORACLE_HOME/dbs/orapw<SID>

# Secure-copy it to the downstream host
scp $ORACLE_HOME/dbs/orapw<SOURCE_SID> oracle@<downstream_host>:$ORACLE_HOME/dbs/orapw<DOWNSTREAM_SID>
```

Confirm `remote_login_passwordfile` is set to `EXCLUSIVE` or `SHARED` on **both** databases:

```sql
SHOW PARAMETER remote_login_passwordfile;
-- Expected: EXCLUSIVE or SHARED
```

---

## Step 3 — Configure Log Transfer on the Source Database

Tell the source database to ship its redo logs to the downstream database asynchronously.

```sql
ALTER SYSTEM SET LOG_ARCHIVE_CONFIG =
    'DG_CONFIG=(db1,strmdb1)';

ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
    'SERVICE=strmdb1
     ASYNC
     NOREGISTER
     VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE)
     DB_UNIQUE_NAME=strmdb1';

ALTER SYSTEM SET LOG_ARCHIVE_DEST_STATE_2 = ENABLE;
```

> **`NOREGISTER`** prevents the downstream from treating these as a Data Guard standby; XStream manages log tracking internally.

---

## Step 4 — Configure RFS (Remote File Server) on the Downstream Database

The downstream database must have an archive destination that accepts the incoming redo files. Run the following on the **downstream** database:

```sql
ALTER SYSTEM SET LOG_ARCHIVE_CONFIG =
    'DG_CONFIG=(db1,strmdb1)';

ALTER SYSTEM SET LOG_ARCHIVE_DEST_2 =
    'LOCATION=/opt/oracle/oradata/STRMDB1/archive_logs/db1
     VALID_FOR=(STANDBY_LOGFILE,PRIMARY_ROLE)';

ALTER SYSTEM SET LOG_ARCHIVE_DEST_STATE_2 = ENABLE;
```

Create the archive log directory if it does not exist:

```bash
mkdir -p /opt/oracle/oradata/STRMDB1/archive_logs/db1
chown oracle:oinstall /opt/oracle/oradata/STRMDB1/archive_logs/db1
```

---

## Step 5 — Add Standby Redo Log Groups on the Downstream Database

Standby redo log (SRL) groups are required for real-time mining. The number and size of SRL groups must match or exceed the online redo log configuration of the source database. Add at least one more SRL group than the maximum number of online redo log groups on the source.

### Single-instance source

```sql
ALTER DATABASE ADD STANDBY LOGFILE GROUP 4
    ('/opt/oracle/oradata/STRMDB1/sredo04.log') SIZE 200M;
ALTER DATABASE ADD STANDBY LOGFILE GROUP 5
    ('/opt/oracle/oradata/STRMDB1/sredo05.log') SIZE 200M;
ALTER DATABASE ADD STANDBY LOGFILE GROUP 6
    ('/opt/oracle/oradata/STRMDB1/sredo06.log') SIZE 200M;
```

### RAC source (one set of SRL groups per RAC thread)

```sql
-- Thread 1
ALTER DATABASE ADD STANDBY LOGFILE THREAD 1 GROUP 6
    ('/opt/oracle/oradata/STRMDB/slog6a.log') SIZE 1G;
ALTER DATABASE ADD STANDBY LOGFILE THREAD 1 GROUP 7
    ('/opt/oracle/oradata/STRMDB/slog7a.log') SIZE 1G;
ALTER DATABASE ADD STANDBY LOGFILE THREAD 1 GROUP 8
    ('/opt/oracle/oradata/STRMDB/slog8a.log') SIZE 1G;
ALTER DATABASE ADD STANDBY LOGFILE THREAD 1 GROUP 9
    ('/opt/oracle/oradata/STRMDB/slog9a.log') SIZE 1G;

-- Thread 2
ALTER DATABASE ADD STANDBY LOGFILE THREAD 2 GROUP 11
    ('/opt/oracle/oradata/STRMDB/slog11a.log') SIZE 1G;
ALTER DATABASE ADD STANDBY LOGFILE THREAD 2 GROUP 12
    ('/opt/oracle/oradata/STRMDB/slog12a.log') SIZE 1G;
ALTER DATABASE ADD STANDBY LOGFILE THREAD 2 GROUP 13
    ('/opt/oracle/oradata/STRMDB/slog13a.log') SIZE 1G;
```

Verify the SRL groups are present:

```sql
SELECT GROUP#, THREAD#, SEQUENCE#, BYTES/1024/1024 AS SIZE_MB, STATUS
FROM   V$STANDBY_LOG
ORDER BY THREAD#, GROUP#;
```

---

## Step 6 — Enable XStream on the Downstream Database

```sql
-- Allocate memory for XStream/Streams processing
ALTER SYSTEM SET streams_pool_size = 2G;

-- Required for XStream capture
ALTER SYSTEM SET enable_goldengate_replication = TRUE;
```

Bounce the downstream instance to apply `streams_pool_size` if it was not set before:

```sql
SHUTDOWN IMMEDIATE;
STARTUP;
```

---

## Step 7 — Create a Database Link on the Downstream Database

The XStream capture process uses this link to retrieve dictionary information from the source database.

```sql
-- Connect as SYS or a DBA user on the downstream database
CREATE PUBLIC DATABASE LINK db1.myoracle.org
    USING 'db1.myoracle.org';
```

Test the link:

```sql
SELECT * FROM GLOBAL_NAME@db1.myoracle.org;
```

---

## Step 8 — Create the XStream Admin and Connector Users

Run the following on **both the source and downstream** databases. These users must exist on both sides.

### XStream administrator (xstrmadmin)

```sql
CREATE USER xstrmadmin IDENTIFIED BY <password>
    DEFAULT TABLESPACE users
    QUOTA UNLIMITED ON users;

GRANT CREATE SESSION TO xstrmadmin;

BEGIN
    DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
        grantee                 => 'xstrmadmin',
        privilege_type          => 'CAPTURE',
        grant_select_privileges => FALSE,
        container               => 'ALL'
    );
END;
/
```

### Connector connect user (cfltuser)

The Confluent connector connects to the outbound server using this user.

```sql
CREATE USER cfltuser IDENTIFIED BY <password>
    DEFAULT TABLESPACE users
    QUOTA UNLIMITED ON users;

GRANT CREATE SESSION       TO cfltuser;
GRANT SELECT_CATALOG_ROLE  TO cfltuser;
GRANT SELECT ANY TABLE     TO cfltuser;
GRANT LOCK ANY TABLE       TO cfltuser;
GRANT FLASHBACK ANY TABLE  TO cfltuser;
```

---

## Step 9 — Create the XStream Outbound Server

Run on the **downstream** database as `xstrmadmin` or `SYS`.

```sql
DECLARE
    v_tables  VARCHAR2(4096) := NULL;        -- NULL = capture all tables in the schema
    v_schemas VARCHAR2(4096) := 'hr';        -- adjust to your target schema(s)
BEGIN
    DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
        server_name     => 'XOUT_DS',
        source_database => 'db1.myoracle.org',
        table_names     => v_tables,
        schema_names    => v_schemas
    );
END;
/
```

### Enable real-time mining on the capture process

```sql
BEGIN
    DBMS_CAPTURE_ADM.SET_PARAMETER(
        capture_name => 'CAP$_XOUT_DS_1',   -- auto-generated name; verify in DBA_CAPTURE
        parameter    => 'downstream_real_time_mine',
        value        => 'Y'
    );
END;
/
```

Confirm the capture name:

```sql
SELECT CAPTURE_NAME, STATUS FROM DBA_CAPTURE;
```

### Assign the connector user to the outbound server

```sql
BEGIN
    DBMS_XSTREAM_ADM.ALTER_OUTBOUND(
        server_name  => 'XOUT_DS',
        connect_user => 'cfltuser'
    );
END;
/
```

---

## Step 10 — Configure the Confluent XStream Source Connector

Point the connector at the **downstream** database:

```json
{
  "connector.class": "io.confluent.connect.oracle.xstream.OracleXStreamSourceConnector",
   "database.hostname": "qyang-dell",
   "database.port": "1521",
   "database.user": "CFLTUSER",
   "database.password": "xxxxx",
   "database.dbname": "DB1",
   "database.service.name": "db1.myoracle.org",

   "downstream.database.hostname": "qyang-dell",
   "downstream.database.dbname": "STRMDB1",
   "downstream.database.port": "1522",
   "downstream.database.service.name": "strmdb1.myoracle.org",

   "database.out.server.name": "XOUT_DS",
   
}
```

---

## Verification

After all steps are complete, verify end-to-end status on the downstream database:

```sql
-- Confirm the capture process is running
SELECT CAPTURE_NAME, STATUS, SOURCE_DATABASE
FROM   DBA_CAPTURE;

-- Confirm the outbound server is enabled
SELECT SERVER_NAME, STATUS, CONNECT_USER, SOURCE_DATABASE
FROM   DBA_XSTREAM_OUTBOUND;

-- Confirm redo is flowing (sequence numbers should advance)
SELECT THREAD#, SEQUENCE#, FIRST_TIME, NEXT_TIME, STATUS
FROM   V$ARCHIVED_LOG
WHERE  STANDBY_DEST = 'YES'
ORDER BY THREAD#, SEQUENCE# DESC
FETCH FIRST 10 ROWS ONLY;
```

---

## Troubleshooting

### ORA-16191 / Error 1017 — password file authentication failure

```
TT00: Error 1017 received logging on to the standby
TT00: Check that the source and target databases are using a password file
      and remote_login_passwordfile is set to SHARED or EXCLUSIVE,
      and that the SYS password is same in the password files
      ORA-16191
```

If you cannot synchronize the `SYS` password across both databases, create a dedicated redo transport user instead:

```sql
-- On BOTH databases
CREATE USER redo_user IDENTIFIED BY <password>;
GRANT SYSOPER TO redo_user;

-- Verify the user appears in the password file
SELECT USERNAME FROM V$PWFILE_USERS;

-- Regenerate the password file on the downstream host to include redo_user
-- (run from the OS shell on the downstream host)
-- orapwd file=$ORACLE_HOME/dbs/orapwSTRMDB password=<sys_password> entries=20 format=12 force=y

-- Set the transport user on the SOURCE database
ALTER SYSTEM SET redo_transport_user = redo_user;
```

### Manually creating the capture queue and process

If `CREATE_OUTBOUND` fails or you need finer control, create the queue and capture process individually on the downstream database:

```sql
-- 1. Create the Streams queue
BEGIN
    DBMS_STREAMS_ADM.SET_UP_QUEUE(
        queue_table => 'xstrm_queue_table',
        queue_name  => 'xstream_queue'
    );
END;
/

-- 2. Build the data dictionary in the redo log (source database)
BEGIN
    DBMS_CAPTURE_ADM.BUILD();
END;
/

-- 3. Create the capture process on the downstream database
BEGIN
    DBMS_CAPTURE_ADM.CREATE_CAPTURE(
        queue_name        => 'xstream_queue',
        capture_name      => 'ds_capture',
        source_database   => 'db1.myoracle.org',
        use_database_link => TRUE,
        capture_class     => 'xstream'
    );
END;
/
```

Set `use_database_link => FALSE` if redo logs are delivered by a mechanism other than an Oracle database link (for example, manual file copy or a shared filesystem).
