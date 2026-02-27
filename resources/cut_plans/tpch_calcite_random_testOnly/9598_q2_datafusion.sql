SELECT COALESCE("NATION", "NATION") AS "NATION", "OVERALL_SALES"
FROM (SELECT "NATION", SUM("TOTAL_SALES") AS "OVERALL_SALES"
FROM (SELECT ANY_VALUE("s1"."n_name") AS "NATION", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SALES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-10-01') AS "t0"
INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "part"."p_partkey"
GROUP BY "s1"."n_name"
UNION ALL
SELECT ANY_VALUE("s10"."n_name") AS "NATION", SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) AS "TOTAL_SALES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1996-10-01') AS "t4"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "t4"."o_orderkey" = "lineitem0"."l_orderkey"
INNER JOIN ("s1" AS "s10" INNER JOIN ("TPCH"."part" AS "part0" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "part0"."p_partkey" = "partsupp0"."ps_partkey") ON "s10"."s_suppkey" = "partsupp0"."ps_suppkey") ON "lineitem0"."l_partkey" = "part0"."p_partkey"
GROUP BY "s10"."n_name") AS "t8"
GROUP BY "NATION"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t10"