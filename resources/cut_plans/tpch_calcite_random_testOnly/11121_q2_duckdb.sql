SELECT COALESCE("t4"."L_ORDERKEY", "t4"."L_ORDERKEY") AS "L_ORDERKEY", "t4"."REVENUE"
FROM (SELECT "lineitem"."l_orderkey" AS "L_ORDERKEY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "s1"."s_suppkey") ON "t1"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "lineitem"."l_orderkey"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"