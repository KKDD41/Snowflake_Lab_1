insert into EPAM_LAB.DATA_MART.NATION_DIM
with nation_with_region as (
    select
        n.n_nationkey,
        n.n_name,
        n.n_comment,
        r.r_name,
        r.r_comment
    from EPAM_LAB.CORE_DWH.NATION n
    left join EPAM_LAB.CORE_DWH.REGION r
        on n.n_regionkey = r.r_regionkey
)
select * from nation_with_region;

insert into EPAM_LAB.DATA_MART.CUSTOMER_DIM
with customer as (
    select
         c_custkey,
         c_name,
         c_address,
         c_nationkey,
         c_phone,
         c_acctbal,
         c_mktsegment,
         c_comment
    from EPAM_LAB.CORE_DWH.CUSTOMER c
)
select * from customer;

insert into EPAM_LAB.DATA_MART.PARTSUPP_DIM
with part_with_supplier as (
    select
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
    from EPAM_LAB.CORE_DWH.PARTSUPP ps
    left join EPAM_LAB.CORE_DWH.PART p
        on ps.ps_partkey = p.p_partkey
    left join EPAM_LAB.CORE_DWH.SUPPLIER s
        on ps.ps_suppkey = s.s_suppkey
)
select * from part_with_supplier;

insert into EPAM_LAB.DATA_MART.LINEITEM_ORDER_FACT
with lineitem_with_order as (
    select
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
    from EPAM_LAB.CORE_DWH.LINEITEM l
    left join EPAM_LAB.CORE_DWH.ORDERS o
        on l.l_orderkey = o.o_orderkey
)
select * from lineitem_with_order;