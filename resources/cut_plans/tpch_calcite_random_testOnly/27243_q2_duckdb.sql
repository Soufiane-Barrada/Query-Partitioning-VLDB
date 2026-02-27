SELECT COALESCE("t6"."p_name", "t6"."p_name") AS "p_name", "t6"."p_brand", "t6"."TOTAL_AVAILABLE_QUANTITY", "t6"."AVERAGE_PRICE", "t6"."TOTAL_ORDERS"
FROM (SELECT "s1"."p_name", "s1"."p_brand", SUM("s1"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("s1"."p_retailprice") AS "AVERAGE_PRICE", COUNT(DISTINCT "s1"."l_orderkey") AS "TOTAL_ORDERS"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE 'Europe%') AS "t3"
INNER JOIN "TPCH"."nation" ON "t3"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_name", "s1"."p_brand"
HAVING SUM("s1"."ps_availqty") > 500
ORDER BY 4 DESC NULLS FIRST) AS "t6"