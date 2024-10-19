-- Procedure for inserting data into NATION_DIM
CREATE OR REPLACE PROCEDURE load_nation_dim()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO EPAM_LAB.DATA_MART.NATION_DIM
    WITH nation_with_region AS (
        SELECT
            n.n_nationkey,
            n.n_name,
            n.n_comment,
            r.r_name,
            r.r_comment
        FROM EPAM_LAB.CORE_DWH.NATION n
        LEFT JOIN EPAM_LAB.CORE_DWH.REGION r
            ON n.n_regionkey = r.r_regionkey
    )
    SELECT * FROM nation_with_region;
    RETURN 'Data loaded into NATION_DIM';
END;

-- Procedure for inserting data into CUSTOMER_DIM
CREATE OR REPLACE PROCEDURE load_customer_dim()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO EPAM_LAB.DATA_MART.CUSTOMER_DIM
    WITH customer AS (
        SELECT
            c_custkey,
            c_name,
            c_address,
            c_nationkey,
            c_phone,
            c_acctbal,
            c_mktsegment,
            c_comment
        FROM EPAM_LAB.CORE_DWH.CUSTOMER c
    )
    SELECT * FROM customer;
    RETURN 'Data loaded into CUSTOMER_DIM';
END;

-- Procedure for inserting data into PARTSUPP_DIM
CREATE OR REPLACE PROCEDURE load_partsupp_dim()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO EPAM_LAB.DATA_MART.PARTSUPP_DIM
    WITH part_with_supplier AS (
        SELECT
            ps.ps_partkey,
            p.p_name,
            p.p_mfgr,
            p.p_brand,
            p.p_type,
            p.p_size,
            p.p_container,
            p.p_retailprice,
            p.p_comment,
            ps.ps_suppkey,
            s.s_name,
            s.s_address,
            s.s_nationkey,
            s.s_phone,
            s.s_acctbal,
            s.s_comment,
            ps.ps_availqty,
            ps.ps_supplycost,
            ps.ps_comment
        FROM EPAM_LAB.CORE_DWH.PARTSUPP ps
        LEFT JOIN EPAM_LAB.CORE_DWH.PART p
            ON ps.ps_partkey = p.p_partkey
        LEFT JOIN EPAM_LAB.CORE_DWH.SUPPLIER s
            ON ps.ps_suppkey = s.s_suppkey
    )
    SELECT * FROM part_with_supplier;
    RETURN 'Data loaded into PARTSUPP_DIM';
END;

-- Procedure for inserting data into LINEITEM_ORDER_FACT
CREATE OR REPLACE PROCEDURE load_lineitem_order_fact()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO EPAM_LAB.DATA_MART.LINEITEM_ORDER_FACT
    WITH lineitem_with_order AS (
        SELECT
            l.l_orderkey,
            o.o_custkey,
            o.o_orderstatus,
            o.o_totalprice,
            o.o_orderdate,
            o.o_orderpriority,
            o.o_clerk,
            o.o_shippriority,
            o.o_comment,
            l.l_partkey,
            l.l_suppkey,
            l.l_linenumber,
            l.l_quantity,
            l.l_extendedprice,
            l.l_discount,
            l.l_tax,
            l.l_returnflag,
            l_linestatus,
            l.l_shipdate,
            l.l_commitdate,
            l.l_receiptdate,
            l.l_shipinstruct,
            l.l_shipmode,
            l.l_comment
        FROM EPAM_LAB.CORE_DWH.LINEITEM l
        LEFT JOIN EPAM_LAB.CORE_DWH.ORDERS o
            ON l.l_orderkey = o.o_orderkey
    )
    SELECT * FROM lineitem_with_order;
    RETURN 'Data loaded into LINEITEM_ORDER_FACT';
END;

CREATE OR REPLACE PROCEDURE reload_data_from_core_dwh_to_data_mart_procedure()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN
    CALL create_tables_in_data_mart('');

    CALL load_nation_dim();
    CALL load_customer_dim();
    CALL load_partsupp_dim();
    CALL load_lineitem_order_fact();

    RETURN 'Tables in DATA_MART were populated with data from CORE_DWH.';
END;