# Oracle XStream CDC Connector Sizing Information Request

To size the Confluent Oracle XStream CDC Connector correctly, we need a few Oracle-side metrics from your DBA team. Please run the queries below on each Oracle database or PDB that will be integrated with CDC and share the results back with the Confluent team.

## Notes before you run the queries

- Please run these queries for each database or PDB that will be captured by the connector.
- For the archive log throughput queries, please update the date range in the `WHERE` clause to reflect a recent representative period. Peak hourly redo generation is one of the most important sizing inputs.
- If you use the table growth query, note that `DBA_HIST_SEG_STAT` is part of an Oracle Management Pack and requires the appropriate Oracle license.
- For the LOB queries, replace `YOUR_SCHEMA` with the Oracle schema that owns the tables you plan to capture.

## 1) CPU / core count

Use these queries to determine the CPU count, core count, and socket count of the Oracle database host.

```sql
-- Amount of CPUs
select value from v$parameter where name like 'cpu_count';

-- or
SELECT TO_CHAR(value) num_cpus
FROM v$osstat
WHERE stat_name = 'NUM_CPUS';

-- Amount of cores
SELECT TO_CHAR(value) num_cores
FROM v$osstat
WHERE stat_name = 'NUM_CPU_CORES';

-- Check the socket count
SELECT TO_CHAR(value) num_sockets
FROM v$osstat
WHERE stat_name = 'NUM_CPU_SOCKETS';
```

## 2) Long-running transactions

Long-running or open transactions are important because uncommitted data requires special handling by the connector.

```sql
-- Open Transactions = Active
select s.username,
       s.program,
       s.status as sessionstatus,
       t.status as transactionstatus,
       t.name,
       t.START_SCN,
       t.START_TIME,
       to_char(sysdate,'MM/DD/YY HH24:MI:SS') as current_time,
       o.sql_text
from v$session s, v$transaction t, v$open_cursor o
where s.taddr = t.addr
  and t.status='ACTIVE'
  and o.sql_id = s.prev_sql_id;

-- Optional
select * from V$SESSION_LONGOPS;
```

## 3) Redo log structure

This helps us understand the redo log group layout, member count, and file sizing.

```sql
SELECT
  a.group#,
  substr(b.member,1,30) name,
  a.members,
  a.bytes/1024/1024 as MB,
  a.status
FROM v$log a,
     v$logfile b
WHERE a.group# = b.group#;
```

## 4) Archive log generation by hour

This query is used to estimate throughput. Please update the date range before running it. We use the peak hourly value to determine the connector throughput requirement.

```sql
set lines 300 pages 300
set num 6
col Day for a9

SELECT
  TRUNC(COMPLETION_TIME),
  THREAD#,
  TO_CHAR(COMPLETION_TIME, 'Day') Day,
  COUNT(1) "Count Files",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '00', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H0",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '01', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H1",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '02', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H2",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '03', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H3",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '04', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H4",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '05', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H5",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '06', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H6",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '07', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H7",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '08', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H8",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '09', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H9",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '10', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H10",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '11', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H11",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '12', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H12",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '13', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H13",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '14', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H14",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '15', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H15",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '16', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H16",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '17', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H17",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '18', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H18",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '19', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H19",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '20', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H20",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '21', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H21",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '22', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H22",
  ROUND(SUM(DECODE(TO_CHAR(COMPLETION_TIME, 'HH24'), '23', ((BLOCKS * BLOCK_SIZE) / 1024 / 1024), 0))) "H23",
  ROUND(SUM(BLOCKS * BLOCK_SIZE) / 1024 / 1024) "Total Size (MB)"
FROM V$ARCHIVED_LOG
WHERE COMPLETION_TIME BETWEEN TO_DATE('01/10/2024', 'DD/MM/YYYY')
                          AND TO_DATE('06/11/2024', 'DD/MM/YYYY')
GROUP BY TRUNC(COMPLETION_TIME), THREAD#, TO_CHAR(COMPLETION_TIME, 'Day')
ORDER BY 1;
```

## 5) Table growth and table size

This helps identify which tables are changing the most and how large the captured tables are overall.

```sql
set lines 200
COLUMN owner FORMAT A15
COLUMN OBJECT_NAME FORMAT A25
COLUMN SUBOBJECT_NAME FORMAT A15
COLUMN OBJECT_TYPE FORMAT A15
COLUMN NAME FORMAT A15
COLUMN bytes HEADING 'Megabytes' FORMAT 9999999

SELECT o.OWNER,
       o.OBJECT_NAME,
       o.SUBOBJECT_NAME,
       o.OBJECT_TYPE,
       t.NAME "Tablespace Name",
       s.growth/(1024*1024) "Growth in MB",
       (SELECT sum(bytes)/(1024*1024)
          FROM dba_segments
         WHERE segment_name=o.object_name) "Total Size(MB)"
FROM DBA_OBJECTS o,
     ( SELECT TS#,OBJ#,
              SUM(SPACE_USED_DELTA) growth
         FROM DBA_HIST_SEG_STAT
        GROUP BY TS#,OBJ#
       HAVING SUM(SPACE_USED_DELTA) > 0
        ORDER BY 2 DESC ) s,
     v$tablespace t
WHERE s.OBJ# = o.OBJECT_ID
  AND o.OBJECT_TYPE = 'TABLE'
  AND s.TS#=t.TS#
ORDER BY 6 DESC;
```

## 6) Log switch frequency

This helps us understand how often redo logs switch and whether redo log sizing may need attention.

```sql
-- Check whether redo log files may be undersized
select *
from V$SYSSTAT
where name = 'redo_buffer_allocation_retries';

-- Time between log switches
select b.recid,
       to_char(b.first_time, 'dd-mon-yy hh:mi:ss') start_time,
       a.recid,
       to_char(a.first_time, 'dd-mon-yy hh:mi:ss') end_time,
       round(((a.first_time-b.first_time)*25)*60,2) minutes
from v$log_history a, v$log_history b
where a.recid = b.recid + 1
order by a.first_time asc;

-- Log switches per hour
SELECT
  substr(to_char(first_time, 'DD-MON-YYYY HH24:MI:SS'),13, 2) as Hour,
  substr(to_char(first_time, 'DD-MON-YYYY HH24:MI:SS'),1, 12) as Day,
  count(1) as Nb_switch_per_hour
FROM v$archived_log
WHERE CAST(FIRST_TIME AS VARCHAR2(20)) LIKE '%1%-APR-23%'
GROUP BY
  substr(to_char(first_time, 'DD-MON-YYYY HH24:MI:SS'),13, 2),
  substr(to_char(first_time, 'DD-MON-YYYY HH24:MI:SS'),1, 12)
ORDER BY Hour ASC;
```

## 7) Identify LOB and special data types

Use this query to identify captured tables that contain LOB or other special data types such as `CLOB`, `BLOB`, `JSON`, `XMLTYPE`, `LONG`, or `LONG RAW`.

```sql
select OWNER||'.'||TABLE_NAME||'('||COLUMN_NAME||' '||DATA_TYPE||')' as mycolumns
from all_tab_columns
where OWNER='YOUR_SCHEMA'
  and data_type in ('CLOB','BLOB','NCLOB','LONG','LONG RAW','XMLTYPE','JSON');
```

## 8) Identify LOB records larger than 20 MB

This block checks for very large LOB records and reports the maximum LOB size by table. Replace `YOUR_SCHEMA` with the relevant schema before running it.

```sql
DECLARE
  v_sql VARCHAR2(4000);
  v_table VARCHAR2(128);
  v_column VARCHAR2(128);
  v_count NUMBER;
BEGIN
  FOR rec IN (
    SELECT table_name, column_name
    FROM all_tab_columns
    WHERE owner = 'YOUR_SCHEMA'
      AND data_type = 'CLOB'
    ORDER BY table_name
  ) LOOP
    v_table := rec.table_name;
    v_column := rec.column_name;
    v_count := 0;

    BEGIN
      v_sql := 'SELECT COUNT(*) FROM YOUR_SCHEMA.' || v_table ||
               ' WHERE DBMS_LOB.GETLENGTH(' || v_column || ') > 20971520';
      EXECUTE IMMEDIATE v_sql INTO v_count;

      IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_table || ', Large Records (> 20MB): ' || v_count);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Skipping ' || v_table || ': ' || SQLERRM);
    END;

    BEGIN
      v_sql := 'SELECT MAX(DBMS_LOB.GETLENGTH(' || v_column || ')) FROM YOUR_SCHEMA.' || v_table;
      EXECUTE IMMEDIATE v_sql INTO v_count;

      IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_table || ', Column: ' || v_column ||
                             ' has max LOB size of ' || v_count/1024/1024 || 'MB');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Skipping ' || v_table || ': ' || SQLERRM);
    END;
  END LOOP;
END;
```

## 9) SGA and memory settings

Please run the following commands to capture the SGA and memory configuration for the Oracle instance.

```sql
PROMPT ==== displays SGA Setup Part 1====
show parameter sga_max_size
show parameter sga_min_size
show parameter sga_target
show parameter pga_aggregate_target

PROMPT ==== displays Automatic Memory Management (AMM) Setup Part 2====
show parameter MEMORY_TARGET
show parameter MEMORY_MAX_TARGET
```

## Please also share these non-query answers

In addition to the query output, please provide the following information:

- Oracle version and edition
- Database type: NON-CDB, CDB, or CDB with PDB
- Number of databases or PDBs to capture
- Number of tables to capture
- Approximate row count and row size for the largest tables
- Change frequency of the captured tables
- Typical transaction size
- Expected throughput and latency requirements
- Whether there are long-running transactions

## Full script reference

If preferred, your DBA team can also review the full script that contains these queries and additional context:

[XStream Info Gathering Script](https://github.com/ora0600/atg-cdc-boost-service/blob/main/pre-flight/Confluent_CDCXStream_info_gathering.sql)

Please send the query outputs and answers back to the Confluent account team for sizing review.
