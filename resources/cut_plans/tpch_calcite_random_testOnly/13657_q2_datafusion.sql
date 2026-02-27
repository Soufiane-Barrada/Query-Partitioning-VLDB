SELECT COALESCE("t3"."NATION", "t3"."NATION") AS "NATION", "t3"."TOTAL_REVENUE"
FROM (SELECT "nation"."n_name", ANY_VALUE("nation"."n_name") AS "NATION", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t0"
INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey") ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "nation"."n_name"
ORDER BY 3 DESC NULLS FIRST) AS "t3"