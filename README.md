# Snowflake Lab 1

## Task 1: DB Creation

### Target schemas creation

Were created database `EPAM_LAB` with two schemas inside:
1. `CORE_DWH`: stores unmodified initial 3NF data, and acts as a silver layer. 
It also has internal Snowflake `STAGE` for raw data loading. 
2. `DATA_MART`: stores denormalized data (star-schema) extracted from `CORE_DWH` tables.

![](./screenshots/1-DB-Schemas-creation.png)

### Target tables in CORE_DWH creation
1. 8 Tables in `CORE_DWH` were created with provided DDL file `./ddl/core_dwh_ddl.sql`.
2. Script was unmodified, i.e. no constraints on PK / FK were given.
3. All DDL statements are wrapped as Stored Procedure `create_tables_in_core_dwh`, which will be used in data reloading ETL.

![](./screenshots/2-Tables-Creation.png)

### Star Schema modelling for DATA_MART
Star Schema for `DATA_MART` consists from 3 dimension tables, and one fact composed in the following way:
1. Shared dimension table `NATION_DIM`, formed from `CORE_DWH.NATION` and `CORE_DWH.REGION` tables.
```sql
create table nation_dim
    (
      n_nationkey INTEGER not null,
      n_name      CHAR(27),
      n_comment   VARCHAR(155),
      r_name      CHAR(25),
      r_comment   VARCHAR(152)
    )
```
2. Dimension table `PARTSUPP_DIM`, which united `CORE_DWH.PART`, `CORE_DWH.PARTSUPP`, and `CORE_DWH.SUPPLIER`.
```sql
create table partsupp_dim
    (
      ps_partkey    INTEGER not null,
      p_name        VARCHAR(55),
      p_mfgr        CHAR(25),
      p_brand       CHAR(10),
      p_type        VARCHAR(25),
      p_size        INTEGER,
      p_container   CHAR(10),
      p_retailprice INTEGER,
      p_comment     VARCHAR(23),
      ps_suppkey    INTEGER not null,
      s_name      CHAR(25),
      s_address   VARCHAR(40),
      s_nationkey INTEGER,
      s_phone     CHAR(15),
      s_acctbal   FLOAT8,
      s_comment   VARCHAR(101),
      ps_availqty   INTEGER,
      ps_supplycost FLOAT8 not null,
      ps_comment    VARCHAR(199)
    )
```
3. Dimension table `CUSTOMER_DIM`, corresponding to `CORE_DWH.CUSTOMER`.
```sql
create table customer_dim
    (
      c_custkey    INTEGER not null,
      c_name       VARCHAR(25),
      c_address    VARCHAR(40),
      c_nationkey  INTEGER,
      c_phone      CHAR(15),
      c_acctbal    FLOAT8,
      c_mktsegment CHAR(10),
      c_comment    VARCHAR(117)
    )
```
4. Fact table `LINEITEM_ORDER_FACT`, corresponding to each order of line-item (`CORE_DWH.LINEITEM` and `CORE_DWH.ORDERS`). 
```sql
    create table lineitem_order_fact
    (
      l_orderkey      INTEGER not null,
      o_custkey       INTEGER not null,
      o_orderstatus   CHAR(1),
      o_totalprice    FLOAT8,
      o_orderdate     DATE,
      o_orderpriority CHAR(15),
      o_clerk         CHAR(15),
      o_shippriority  INTEGER,
      o_comment       VARCHAR(79),
      l_partkey       INTEGER not null,
      l_suppkey       INTEGER not null,
      l_linenumber    INTEGER not null,
      l_quantity      INTEGER not null,
      l_extendedprice FLOAT8 not null,
      l_discount      FLOAT8 not null,
      l_tax           FLOAT8 not null,
      l_returnflag    CHAR(1),
      l_linestatus    CHAR(1),
      l_shipdate      DATE,
      l_commitdate    DATE,
      l_receiptdate   DATE,
      l_shipinstruct  CHAR(25),
      l_shipmode      CHAR(10),
      l_comment       VARCHAR(44)
    )
```
### Tables creation in DATA_MART
1. 4 Described tables in `DATA_MART` were created with provided DDL file `./ddl/data_mart_ddl.sql`.
2. In the same way as for `CORE_DWH` schema, no constraints on PK / FK were given.
3. All DDL statements are wrapped as Stored Procedure `create_tables_in_data_mart`, which will be used in data reloading ETL.

![](./screenshots/6-Tables-Creation-Data-Mart.png)


## Task 2: Data Loading

### Data Preprocessing

### Loading files to Snowflake Stage
![](./screenshots/3-Stage-Creation.png)
![](./screenshots/4-Loading-Files-to-Stage.png)

### Copy data to CORE_DWH tables
![](./screenshots/5-Copy-from-Stage-to-Core-DWH.png)

## Task 3: ETL Data Workflow

### Copy data into DATA_MART tables
![](./screenshots/7-Inserting-Data-Into-Data-Mart.png)

### Workflow Automation
![](./screenshots/9-Create-scheduled-load-to-Core-Dwh-with-task.png)
![](./screenshots/10-All-tasks-graph.png)


## Task 4: Connection to BI-Tool

## Task 5: Snowflake SQL

## Task 6: Other Snowflake Features

## Task 7: Snowpipe