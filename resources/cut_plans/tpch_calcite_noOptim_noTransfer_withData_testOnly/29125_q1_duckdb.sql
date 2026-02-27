SELECT COALESCE("t1"."S_NAME", "t1"."S_NAME") AS "SUPPLIER_NAME", "t1"."NATION" AS "SUPPLIER_NATION", "t1"."TOTAL_PARTS" AS "PARTS_SUPPLIED", "t1"."TOTAL_SUPPLYCOST" AS "TOTAL_SUPPLY_COST", "t3"."C_NAME" AS "CUSTOMER_NAME", "t3"."TOTAL_ORDERS" AS "TOTAL_ORDERS_PLACED", "t3"."TOTAL_SPENT" AS "TOTAL_SPENT_BY_CUSTOMER"
FROM (SELECT "supplier"."s_suppkey" AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", ANY_VALUE("nation"."n_name") AS "NATION", COUNT(DISTINCT "part"."p_partkey") AS "TOTAL_PARTS", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name", "nation"."n_name") AS "t1"
INNER JOIN (SELECT "customer"."c_custkey" AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", SUM("orders"."o_totalprice") AS "TOTAL_SPENT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t3" ON "t1"."TOTAL_PARTS" = (((SELECT MAX(COUNT(DISTINCT "part0"."p_partkey"))
FROM "TPCH"."supplier" AS "supplier0"
INNER JOIN "TPCH"."nation" AS "nation0" ON "supplier0"."s_nationkey" = "nation0"."n_nationkey"
INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey"
INNER JOIN "TPCH"."part" AS "part0" ON "partsupp0"."ps_partkey" = "part0"."p_partkey"
GROUP BY "supplier0"."s_suppkey", "supplier0"."s_name", "nation0"."n_name")))
WHERE "t1"."TOTAL_SUPPLYCOST" > 10000.00