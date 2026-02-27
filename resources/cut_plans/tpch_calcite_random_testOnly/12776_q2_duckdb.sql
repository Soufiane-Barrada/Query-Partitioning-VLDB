SELECT COALESCE("t4"."REVENUE", "t4"."REVENUE") AS "REVENUE", "t4"."N_NAME", "t4"."O_YEAR"
FROM (SELECT "nation"."n_name" AS "N_NAME", EXTRACT(YEAR FROM "s1"."o_orderdate") AS "$f1", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", ANY_VALUE(EXTRACT(YEAR FROM "s1"."o_orderdate")) AS "O_YEAR"
FROM "s1"
INNER JOIN "TPCH"."lineitem" ON "s1"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_brand" = 'Brand#22') AS "t1" INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"
GROUP BY "nation"."n_name", EXTRACT(YEAR FROM "s1"."o_orderdate")
ORDER BY 3 DESC NULLS FIRST) AS "t4"