SELECT COALESCE("t3"."N_NAME", "t3"."N_NAME") AS "N_NAME", "t3"."TOTAL_REVENUE"
FROM (SELECT "nation"."n_name" AS "N_NAME", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1994-01-01' AND "o_orderdate" < DATE '1995-01-01') AS "t0"
INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey") ON "t0"."o_custkey" = "customer"."c_custkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "t0"."o_orderkey" = "s1"."l_orderkey"
GROUP BY "nation"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"