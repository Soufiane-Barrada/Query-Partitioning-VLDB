SELECT COALESCE("customer"."c_custkey", "customer"."c_custkey") AS "c_custkey", "customer"."c_name", "customer"."c_address", "customer"."c_nationkey", "customer"."c_phone", "customer"."c_acctbal", "customer"."c_mktsegment", "customer"."c_comment", "t"."o_orderkey", "t"."o_custkey", "t"."o_orderstatus", "t"."o_totalprice", "t"."o_orderdate", "t"."o_orderpriority", "t"."o_clerk", "t"."o_shippriority", "t"."o_comment", "t0"."l_orderkey", "t0"."l_partkey", "t0"."l_suppkey", "t0"."l_linenumber", "t0"."l_quantity", "t0"."l_extendedprice", "t0"."l_discount", "t0"."l_tax", "t0"."l_returnflag", "t0"."l_linestatus", "t0"."l_shipdate", "t0"."l_commitdate", "t0"."l_receiptdate", "t0"."l_shipinstruct", "t0"."l_shipmode", "t0"."l_comment", "t1"."p_partkey", "t1"."p_name", "t1"."p_mfgr", "t1"."p_brand", "t1"."p_type", "t1"."p_size", "t1"."p_container", "t1"."p_retailprice", "t1"."p_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t" ON "customer"."c_custkey" = "t"."o_custkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'TRUCK')) AS "t0" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."l_partkey" = "partsupp"."ps_partkey") ON "t"."o_orderkey" = "t0"."l_orderkey"