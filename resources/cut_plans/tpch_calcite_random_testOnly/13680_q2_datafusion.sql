SELECT COALESCE("t3"."P_PARTKEY", "t3"."P_PARTKEY") AS "P_PARTKEY", "t3"."P_NAME", "t3"."S_NAME", "t3"."REVENUE"
FROM (SELECT "s1"."p_partkey" AS "P_PARTKEY", "s1"."p_name" AS "P_NAME", "supplier"."s_name" AS "S_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t0"
INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."supplier" INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey") ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
GROUP BY "s1"."p_partkey", "s1"."p_name", "supplier"."s_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"