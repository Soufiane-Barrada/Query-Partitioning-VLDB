SELECT COALESCE("t5"."PART_NAME", "t5"."PART_NAME") AS "PART_NAME", "t5"."ORDER_COUNT", "t5"."TOTAL_REVENUE", "t5"."REGION_NAME", "t5"."NATION_NAME"
FROM (SELECT "t1"."p_name", "region"."r_name", "nation"."n_name", ANY_VALUE("t1"."p_name") AS "PART_NAME", COUNT(DISTINCT "s1"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%BRASS%') AS "t1" INNER JOIN ("s1" INNER JOIN "TPCH"."lineitem" ON "s1"."o_orderkey" = "lineitem"."l_orderkey") ON "t1"."p_partkey" = "lineitem"."l_partkey") ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
GROUP BY "t1"."p_name", "region"."r_name", "nation"."n_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 10000.0000
ORDER BY 6 DESC NULLS FIRST, 4) AS "t5"