SELECT COALESCE("t4"."P_PARTKEY", "t4"."P_PARTKEY") AS "P_PARTKEY", "t4"."P_NAME", "t4"."REVENUE"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", "part"."p_name" AS "P_NAME", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "REVENUE"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t1" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."l_partkey" = "part"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"