SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."SUPPLIER_NAME", "t4"."TOTAL_QUANTITY", "t4"."TOTAL_REVENUE", "t4"."TOTAL_ORDERS", "t4"."LAST_SHIP_DATE", "t4"."FIRST_SHIP_DATE", "t4"."AVERAGE_DISCOUNT", "t4"."REGIONS_SUPPLY", "t4"."SHORT_COMMENT"
FROM (SELECT "s1"."p_name" AS "P_NAME", "supplier"."s_name", "s1"."p_comment", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", SUM("t0"."l_quantity") AS "TOTAL_QUANTITY", SUM("t0"."l_extendedprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", MAX("t0"."l_shipdate") AS "LAST_SHIP_DATE", MIN("t0"."l_shipdate") AS "FIRST_SHIP_DATE", AVG("t0"."l_discount") AS "AVERAGE_DISCOUNT", LISTAGG(DISTINCT "region"."r_name", ', ') AS "REGIONS_SUPPLY", ANY_VALUE(SUBSTRING("s1"."p_comment", 1, 10)) AS "SHORT_COMMENT"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "s1"."p_partkey" = "t0"."l_partkey"
GROUP BY "s1"."p_name", "supplier"."s_name", "s1"."p_comment"
HAVING SUM("t0"."l_quantity") > 100.00
ORDER BY 6 DESC NULLS FIRST) AS "t4"