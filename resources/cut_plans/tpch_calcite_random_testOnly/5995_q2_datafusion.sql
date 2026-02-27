SELECT COALESCE("t7"."SUPPLIER_NAME", "t7"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t7"."CUSTOMER_NAME", "t7"."TOTAL_SPENT"
FROM (SELECT "t1"."s_name" AS "SUPPLIER_NAME", "t4"."C_NAME" AS "CUSTOMER_NAME", "t4"."TOTAL_SPENT"
FROM (SELECT "s_suppkey", "s_name", "TOTAL_SUPPLYCOST"
FROM "s1"
ORDER BY "TOTAL_SUPPLYCOST" DESC NULLS FIRST
FETCH NEXT 5 ROWS ONLY) AS "t1"
INNER JOIN ((SELECT "customer0"."c_custkey" AS "C_CUSTKEY", "customer0"."c_name" AS "C_NAME", "t2"."ORDER_COUNT", "t2"."TOTAL_SPENT"
FROM (SELECT "customer"."c_custkey", "customer"."c_name", COUNT(*) AS "ORDER_COUNT", SUM("orders"."o_totalprice") AS "TOTAL_SPENT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t2"
INNER JOIN "TPCH"."customer" AS "customer0" ON "t2"."c_custkey" = "customer0"."c_custkey"
ORDER BY "t2"."TOTAL_SPENT" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t5" INNER JOIN "TPCH"."orders" AS "orders0" ON "t5"."l_orderkey" = "orders0"."o_orderkey") ON "t4"."C_CUSTKEY" = "orders0"."o_custkey") ON "t1"."s_suppkey" = "t5"."l_suppkey"
ORDER BY "t1"."s_name", "t4"."TOTAL_SPENT" DESC NULLS FIRST) AS "t7"