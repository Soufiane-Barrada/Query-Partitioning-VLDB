SELECT COALESCE("t4"."N_NAME", "t4"."N_NAME") AS "N_NAME", "t4"."TOTAL_REVENUE", "t4"."ORDERS_COUNT"
FROM (SELECT "s1"."n_name" AS "N_NAME", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDERS_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t0"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'TRUCK')) AS "t1" ON "t0"."o_orderkey" = "t1"."l_orderkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."l_partkey" = "part"."p_partkey"
GROUP BY "s1"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"