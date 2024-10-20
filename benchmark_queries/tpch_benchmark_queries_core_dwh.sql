-- 1
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem_order_fact
where
	l_shipdate <= date '1998-12-01' -- interval '91' day (3)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;




--2
select
	s_acctbal,
	s_name,
	n_name,
	ps_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	partsupp_dim,
	nation_dim
where
	s_nationkey = n_nationkey
	and p_size = 45
	and p_type like '%BRASS'
	and r_name = 'AFRICA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp_dim,
			nation_dim,
		where
			s_nationkey = n_nationkey
			and r_name = 'AFRICA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	ps_partkey
	limit 100;



--3
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer_dim,
	lineitem_order_fact
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = l_orderkey
	and o_orderdate < date '1995-03-01'
	and l_shipdate > date '1995-03-01'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;



--4
select
	o_orderpriority,
	count(*) as order_count
from
	lineitem_order_fact
where
	o_orderdate >= date '1993-10-01'
	and o_orderdate < add_months(date '1993-10-01',3)
	and exists (
		select
			*
		from
			lineitem_order_fact
		where
			l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;


--6
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem_order_fact
where
	l_shipdate >= date '1996-01-01'
	and l_shipdate < add_months(date '1996-01-01',12)
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;




--7
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			partsupp_dim,
			lineitem_order_fact,
			customer_dim,
			nation_dim n1,
			nation_dim n2
		where
			ps_suppkey = l_suppkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'INDIA' and n2.n_name = 'SAUDI ARABIA')
				or (n1.n_name = 'SAUDI ARABIA' and n2.n_name = 'INDIA')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;




--8
select
	o_year,
	sum(case
		when nation = 'CHINA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			partsupp_dim,
			lineitem_order_fact,
			customer_dim,
			nation_dim n1,
			nation_dim n2
		where
			ps_partkey = l_partkey
			and ps_suppkey = l_suppkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.r_name = 'ASIA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'STANDARD BURNISHED BRASS'
	) as all_nations
group by
	o_year
order by
	o_year;




--9
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			partsupp_dim,
			lineitem_order_fact,
			nation_dim
		where
			ps_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and s_nationkey = n_nationkey
			and p_name like '%thistle%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;




--10
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer_dim,
	lineitem_order_fact,
	nation_dim
where
	c_custkey = o_custkey
	and o_orderdate >= date '1993-06-01'
	and o_orderdate < add_months(date '1993-06-01',3)
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;
