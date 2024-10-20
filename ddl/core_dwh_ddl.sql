CREATE OR REPLACE PROCEDURE create_tables_in_core_dwh(message VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN
    drop table region;
    create table region
    (
      r_regionkey INTEGER,
      r_name      CHAR(25),
      r_comment   VARCHAR(152)
    );
    
    
    drop table nation;
    create table nation
    (
      n_nationkey INTEGER not null,
      n_name      CHAR(27),
      n_regionkey INTEGER,
      n_comment   VARCHAR(155)
    );
    
    
    drop table supplier;
    create table supplier
    (
      s_suppkey   INTEGER not null,
      s_name      CHAR(25),
      s_address   VARCHAR(40),
      s_nationkey INTEGER,
      s_phone     CHAR(15),
      s_acctbal   FLOAT8,
      s_comment   VARCHAR(101)
    );
    
    
    drop table orders;
    create table orders
    (
      o_orderkey      INTEGER not null,
      o_custkey       INTEGER not null,
      o_orderstatus   CHAR(1),
      o_totalprice    FLOAT8,
      o_orderdate     DATE,
      o_orderpriority CHAR(15),
      o_clerk         CHAR(15),
      o_shippriority  INTEGER,
      o_comment       VARCHAR(79)
    );
    
    
    drop table partsupp;
    create table partsupp
    (
      ps_partkey    INTEGER not null,
      ps_suppkey    INTEGER not null,
      ps_availqty   INTEGER,
      ps_supplycost FLOAT8 not null,
      ps_comment    VARCHAR(199)
    );
    
    
    drop table part;
    create table part
    (
      p_partkey     INTEGER not null,
      p_name        VARCHAR(55),
      p_mfgr        CHAR(25),
      p_brand       CHAR(10),
      p_type        VARCHAR(25),
      p_size        INTEGER,
      p_container   CHAR(10),
      p_retailprice INTEGER,
      p_comment     VARCHAR(23)
    );
    
    
    drop table customer;
    create table customer
    (
      c_custkey    INTEGER not null,
      c_name       VARCHAR(25),
      c_address    VARCHAR(40),
      c_nationkey  INTEGER,
      c_phone      CHAR(15),
      c_acctbal    FLOAT8,
      c_mktsegment CHAR(10),
      c_comment    VARCHAR(117)
    );
    
    
    drop table lineitem;
    create table lineitem
    (
      l_orderkey      INTEGER not null,
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
    );

  RETURN 'Tables in CORE_DWH were created.';
END;
