SELECT COALESCE("t"."r_regionkey", "t"."r_regionkey") AS "r_regionkey", "t"."r_name", "t"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment", "customer"."c_custkey", "customer"."c_name", "customer"."c_address", "customer"."c_nationkey", "customer"."c_phone", "customer"."c_acctbal", "customer"."c_mktsegment", "customer"."c_comment", "t0"."o_orderkey", "t0"."o_custkey", "t0"."o_orderstatus", "t0"."o_totalprice", "t0"."o_orderdate", "t0"."o_orderpriority", "t0"."o_clerk", "t0"."o_shippriority", "t0"."o_comment", "t1"."l_orderkey", "t1"."l_partkey", "t1"."l_suppkey", "t1"."l_linenumber", "t1"."l_quantity", "t1"."l_extendedprice", "t1"."l_discount", "t1"."l_tax", "t1"."l_returnflag", "t1"."l_linestatus", "t1"."l_shipdate", "t1"."l_commitdate", "t1"."l_receiptdate", "t1"."l_shipinstruct", "t1"."l_shipmode", "t1"."l_comment", "t2"."p_partkey", "t2"."p_name", "t2"."p_mfgr", "t2"."p_brand", "t2"."p_type", "t2"."p_size", "t2"."p_container", "t2"."p_retailprice", "t2"."p_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE 'Europe%') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'TRUCK')) AS "t1" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t2" INNER JOIN "TPCH"."partsupp" ON "t2"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."l_partkey" = "partsupp"."ps_partkey") ON "t0"."o_orderkey" = "t1"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"