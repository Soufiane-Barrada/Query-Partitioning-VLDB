SELECT COALESCE("t3"."N_NAME", "t3"."N_NAME") AS "N_NAME", "t3"."REVENUE"
FROM (SELECT "s1"."n_name" AS "N_NAME", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "REVENUE"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey"
INNER JOIN "s1" ON "t0"."o_orderkey" = "s1"."l_orderkey"
GROUP BY "s1"."n_name"
ORDER BY 2 DESC NULLS FIRST) AS "t3"