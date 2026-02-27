SELECT COALESCE("s1"."S_NAME", "s1"."S_NAME") AS "SUPPLIER_NAME", "s1"."NATION" AS "SUPPLIER_NATION", "s1"."TOTAL_PARTS" AS "PARTS_SUPPLIED", "s1"."TOTAL_SUPPLYCOST" AS "TOTAL_SUPPLY_COST", "t4"."C_NAME" AS "CUSTOMER_NAME", "t4"."TOTAL_ORDERS" AS "TOTAL_ORDERS_PLACED", "t4"."TOTAL_SPENT" AS "TOTAL_SPENT_BY_CUSTOMER"
FROM "s1"
INNER JOIN (SELECT "customer"."c_custkey" AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", SUM("orders"."o_totalprice") AS "TOTAL_SPENT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t4" ON "s1"."TOTAL_PARTS" = (((SELECT MAX(COUNT(DISTINCT "part0"."p_partkey"))
FROM "TPCH"."supplier" AS "supplier0"
INNER JOIN "TPCH"."nation" AS "nation0" ON "supplier0"."s_nationkey" = "nation0"."n_nationkey"
INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey"
INNER JOIN "TPCH"."part" AS "part0" ON "partsupp0"."ps_partkey" = "part0"."p_partkey"
GROUP BY "supplier0"."s_suppkey", "supplier0"."s_name", "nation0"."n_name")))
WHERE "s1"."TOTAL_SUPPLYCOST" > 10000.00
ORDER BY "t4"."TOTAL_SPENT" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY