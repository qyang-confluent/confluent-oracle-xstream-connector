# SQL Queries to collect Oracle XStream CDC Connector Sizing Information 

To size the Confluent Oracle XStream CDC Connector correctly, we need a few Oracle-side metrics from your DBA team. Please run the queries below on each Oracle database or PDB that will be integrated with CDC and share the results back with the Confluent team.

## Notes before you run the queries

- Please run these queries for each database or PDB that will be captured by the connector.  
- For the archive log throughput queries and run in CDB, please update the date range in the `WHERE` clause to reflect a recent representative period. Peak hourly redo generation is one of the most important sizing inputs.  
- If you use the table growth query, note that `DBA_HIST_SEG_STAT` is part of an Oracle Management Pack and requires the appropriate Oracle license.  

## 1) Redo log structure

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

## 2) Archive log generation by hour

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

## 3) Table growth and table size

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

## 4) Log switch frequency

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


## 5) SGA and memory settings

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


## Please also share these non-query answers if the connector is going to run against a new Oracle Database

- Oracle version and edition  
- Database type: NON-CDB, CDB, or CDB with PDB  
- Number of databases or PDBs to capture  
- Number of tables to capture  
- Approximate row count and row size for the largest tables  
- Change frequency of the captured tables  
- Typical transaction size  
- Expected throughput and latency requirements  
- Whether there are long-running transactions

