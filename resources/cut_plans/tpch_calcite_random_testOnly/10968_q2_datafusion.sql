SELECT COALESCE("t3"."N_NAME", "t3"."N_NAME") AS "N_NAME", "t3"."TOTAL_REVENUE"
FROM (SELECT "s1"."n_name" AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01') AS "t0"
INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN "s1" ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."n_name"
ORDER BY 2 DESC NULLS FIRST) AS "t3"