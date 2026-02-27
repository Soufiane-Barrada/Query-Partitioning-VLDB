SELECT COALESCE("t3"."NATION", "t3"."NATION") AS "NATION", "t3"."REGION", "t3"."TOTAL_REVENUE"
FROM (SELECT "s1"."n_name", "s1"."r_name", ANY_VALUE("s1"."n_name") AS "NATION", ANY_VALUE("s1"."r_name") AS "REGION", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-02-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("s1" INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "supplier"."s_suppkey") ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "s1"."n_name", "s1"."r_name"
ORDER BY 5 DESC NULLS FIRST) AS "t3"