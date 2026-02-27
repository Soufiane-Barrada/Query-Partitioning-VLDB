SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "p_name", "t1"."TOTAL_QUANTITY", "t1"."TOTAL_REVENUE", "t1"."TOTAL_ORDERS"
FROM (SELECT "part"."p_name", SUM("s1"."l_quantity") AS "TOTAL_QUANTITY", SUM("s1"."l_extendedprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS"
FROM "TPCH"."part"
INNER JOIN "s1" ON "part"."p_partkey" = "s1"."l_partkey"
GROUP BY "part"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t1"