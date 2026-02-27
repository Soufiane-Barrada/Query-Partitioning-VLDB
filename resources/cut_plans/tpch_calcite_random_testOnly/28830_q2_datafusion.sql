SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."SUPPLIER_INFO", "t4"."ORDER_COUNT", "t4"."AVG_ORDER_VALUE", "t4"."SHIP_MODES", "t4"."P_NAME" AS "p_name_"
FROM (SELECT "s1"."p_name" AS "P_NAME", CONCAT("supplier"."s_name", ' ', "supplier"."s_address", ', ', "region"."r_name") AS "$f1", ANY_VALUE(CONCAT("supplier"."s_name", ' ', "supplier"."s_address", ', ', "region"."r_name")) AS "SUPPLIER_INFO", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT", AVG("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "AVG_ORDER_VALUE", LISTAGG(DISTINCT "t1"."l_shipmode", ', ') AS "SHIP_MODES"
FROM (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1"
INNER JOIN "TPCH"."orders" ON "t1"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey") ON "t1"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."p_name", CONCAT("supplier"."s_name", ' ', "supplier"."s_address", ', ', "region"."r_name")
ORDER BY 4 DESC NULLS FIRST, 5 DESC NULLS FIRST, "s1"."p_name") AS "t4"