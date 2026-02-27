SELECT COALESCE("t0"."s_suppkey", "t0"."s_suppkey") AS "S_SUPPKEY", "t0"."s_name" AS "S_NAME", "t0"."TOTAL_SUPPLYCOST", "t3"."C_CUSTKEY", "t3"."C_NAME", "t3"."ORDER_COUNT", "t3"."TOTAL_SPENT", "t4"."l_orderkey", "t4"."l_partkey", "t4"."l_suppkey", "t4"."l_linenumber", "t4"."l_quantity", "t4"."l_extendedprice", "t4"."l_discount", "t4"."l_tax", "t4"."l_returnflag", "t4"."l_linestatus", "t4"."l_shipdate", "t4"."l_commitdate", "t4"."l_receiptdate", "t4"."l_shipinstruct", "t4"."l_shipmode", "t4"."l_comment", "orders0"."o_orderkey", "orders0"."o_custkey", "orders0"."o_orderstatus", "orders0"."o_totalprice", "orders0"."o_orderdate", "orders0"."o_orderpriority", "orders0"."o_clerk", "orders0"."o_shippriority", "orders0"."o_comment"
FROM (SELECT "supplier"."s_suppkey", "supplier"."s_name", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 5 ROWS ONLY) AS "t0"
INNER JOIN ((SELECT "customer0"."c_custkey" AS "C_CUSTKEY", "customer0"."c_name" AS "C_NAME", "t1"."ORDER_COUNT", "t1"."TOTAL_SPENT"
FROM (SELECT "customer"."c_custkey", "customer"."c_name", COUNT(*) AS "ORDER_COUNT", SUM("orders"."o_totalprice") AS "TOTAL_SPENT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t1"
INNER JOIN "TPCH"."customer" AS "customer0" ON "t1"."c_custkey" = "customer0"."c_custkey"
ORDER BY "t1"."TOTAL_SPENT" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t4" INNER JOIN "TPCH"."orders" AS "orders0" ON "t4"."l_orderkey" = "orders0"."o_orderkey") ON "t3"."C_CUSTKEY" = "orders0"."o_custkey") ON "t0"."s_suppkey" = "t4"."l_suppkey"