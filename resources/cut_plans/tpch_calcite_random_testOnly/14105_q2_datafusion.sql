SELECT COALESCE("t3"."NATION", "t3"."NATION") AS "NATION", "t3"."REGION", "t3"."TOTAL_REVENUE"
FROM (SELECT "s1"."n_name", "s1"."r_name", ANY_VALUE("s1"."n_name") AS "NATION", ANY_VALUE("s1"."r_name") AS "REGION", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."lineitem"
INNER JOIN ("s1" INNER JOIN "TPCH"."customer" ON "s1"."n_nationkey" = "customer"."c_nationkey" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey") ON "lineitem"."l_orderkey" = "t0"."o_orderkey"
GROUP BY "s1"."n_name", "s1"."r_name"
ORDER BY 5 DESC NULLS FIRST) AS "t3"