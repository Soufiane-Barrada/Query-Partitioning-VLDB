SELECT COALESCE("t2"."P_PARTKEY", "t2"."P_PARTKEY") AS "P_PARTKEY", "t2"."P_NAME", "t2"."S_NAME", "t2"."TOTAL_QUANTITY", "t2"."REVENUE"
FROM (SELECT "s1"."p_partkey" AS "P_PARTKEY", "s1"."p_name" AS "P_NAME", "s1"."s_name" AS "S_NAME", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM "TPCH"."orders"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN "s1" ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."p_partkey", "s1"."p_name", "s1"."s_name"
ORDER BY 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"