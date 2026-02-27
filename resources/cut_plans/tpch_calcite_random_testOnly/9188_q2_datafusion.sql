SELECT COALESCE("t9"."REGION_NAME", "t9"."REGION_NAME") AS "REGION_NAME", "t9"."TOTAL_SALES", "t9"."ORDER_COUNT", "t9"."SALES_VARIANCE_PERCENTAGE", "t9"."ORDER_COUNT_VARIANCE_PERCENTAGE"
FROM (SELECT "t7"."REGION_NAME", "t7"."TOTAL_SALES", "t7"."ORDER_COUNT", ("t7"."TOTAL_SALES" - "t3"."AVG_SALES") / CASE WHEN "t3"."AVG_SALES" = 0.0000 THEN NULL ELSE "t3"."AVG_SALES" END * 100 AS "SALES_VARIANCE_PERCENTAGE", ("t7"."ORDER_COUNT" - "t3"."AVG_ORDER_COUNT") / CASE WHEN "t3"."AVG_ORDER_COUNT" = 0 THEN NULL ELSE "t3"."AVG_ORDER_COUNT" END * 100 AS "ORDER_COUNT_VARIANCE_PERCENTAGE"
FROM (SELECT AVG("t2"."TOTAL_SALES") AS "AVG_SALES", AVG("t2"."ORDER_COUNT") AS "AVG_ORDER_COUNT"
FROM (SELECT "s1"."r_name", ANY_VALUE("s1"."r_name") AS "REGION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SALES", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDER_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t0"
INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "part"."p_partkey"
GROUP BY "s1"."r_name") AS "t2") AS "t3",
(SELECT ANY_VALUE("s10"."r_name") AS "REGION_NAME", SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) AS "TOTAL_SALES", COUNT(DISTINCT "t4"."o_orderkey") AS "ORDER_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t4"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "t4"."o_orderkey" = "lineitem0"."l_orderkey"
INNER JOIN ("s1" AS "s10" INNER JOIN "TPCH"."supplier" AS "supplier0" ON "s10"."n_nationkey" = "supplier0"."s_nationkey" INNER JOIN ("TPCH"."part" AS "part0" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "part0"."p_partkey" = "partsupp0"."ps_partkey") ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey") ON "lineitem0"."l_partkey" = "part0"."p_partkey"
GROUP BY "s10"."r_name") AS "t7"
ORDER BY "t7"."TOTAL_SALES" DESC NULLS FIRST) AS "t9"