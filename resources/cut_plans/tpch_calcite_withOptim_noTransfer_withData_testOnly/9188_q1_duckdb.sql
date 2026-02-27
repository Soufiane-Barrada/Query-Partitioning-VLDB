SELECT COALESCE("t2"."AVG_SALES", "t2"."AVG_SALES") AS "AVG_SALES", "t2"."AVG_ORDER_COUNT", "t6"."REGION_NAME", "t6"."TOTAL_SALES", "t6"."ORDER_COUNT"
FROM (SELECT AVG("t1"."TOTAL_SALES") AS "AVG_SALES", AVG("t1"."ORDER_COUNT") AS "AVG_ORDER_COUNT"
FROM (SELECT "region"."r_name", ANY_VALUE("region"."r_name") AS "REGION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SALES", COUNT(DISTINCT "t"."o_orderkey") AS "ORDER_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "part"."p_partkey"
GROUP BY "region"."r_name") AS "t1") AS "t2",
(SELECT ANY_VALUE("region0"."r_name") AS "REGION_NAME", SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) AS "TOTAL_SALES", COUNT(DISTINCT "t3"."o_orderkey") AS "ORDER_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t3"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "t3"."o_orderkey" = "lineitem0"."l_orderkey"
INNER JOIN ("TPCH"."region" AS "region0" INNER JOIN "TPCH"."nation" AS "nation0" ON "region0"."r_regionkey" = "nation0"."n_regionkey" INNER JOIN "TPCH"."supplier" AS "supplier0" ON "nation0"."n_nationkey" = "supplier0"."s_nationkey" INNER JOIN ("TPCH"."part" AS "part0" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "part0"."p_partkey" = "partsupp0"."ps_partkey") ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey") ON "lineitem0"."l_partkey" = "part0"."p_partkey"
GROUP BY "region0"."r_name") AS "t6"