SELECT COALESCE("t3"."N_NAME", "t3"."N_NAME") AS "N_NAME", "t3"."REVENUE"
FROM (SELECT "s1"."n_name" AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-02-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN "s1" ON "lineitem"."l_partkey" = "s1"."ps_partkey" AND "lineitem"."l_suppkey" = "s1"."s_suppkey") ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "s1"."n_name"
ORDER BY 2 DESC NULLS FIRST) AS "t3"