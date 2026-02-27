SELECT COALESCE("t2"."REVENUE", "t2"."REVENUE") AS "REVENUE", "t2"."NATION"
FROM (SELECT "nation"."n_name", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", ANY_VALUE("nation"."n_name") AS "NATION"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
INNER JOIN "s1" ON "lineitem"."l_orderkey" = "s1"."o_orderkey"
GROUP BY "nation"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"