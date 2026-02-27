SELECT COALESCE("t"."o_orderkey", "t"."o_orderkey") AS "o_orderkey", "t"."o_custkey", "t"."o_orderstatus", "t"."o_totalprice", "t"."o_orderdate", "t"."o_orderpriority", "t"."o_clerk", "t"."o_shippriority", "t"."o_comment", "t0"."N_NATIONKEY", "t0"."N_NAME", "t0"."R_NAME", "customer"."c_custkey", "customer"."c_name", "customer"."c_address", "customer"."c_nationkey", "customer"."c_phone", "customer"."c_acctbal", "customer"."c_mktsegment", "customer"."c_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment", "t1"."l_orderkey", "t1"."l_partkey", "t1"."l_suppkey", "t1"."l_linenumber", "t1"."l_quantity", "t1"."l_extendedprice", "t1"."l_discount", "t1"."l_tax", "t1"."l_returnflag", "t1"."l_linestatus", "t1"."l_shipdate", "t1"."l_commitdate", "t1"."l_receiptdate", "t1"."l_shipinstruct", "t1"."l_shipmode", "t1"."l_comment"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'F') AS "t"
INNER JOIN ((SELECT "nation"."n_nationkey" AS "N_NATIONKEY", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey") AS "t0" INNER JOIN "TPCH"."customer" ON "t0"."N_NATIONKEY" = "customer"."c_nationkey") ON "t"."o_custkey" = "customer"."c_custkey"
INNER JOIN ("TPCH"."partsupp" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" ON "partsupp"."ps_partkey" = "t1"."l_partkey") ON "t"."o_orderkey" = "t1"."l_orderkey"