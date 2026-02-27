SELECT COALESCE("t"."p_name", "t"."p_name") AS "p_name", "region"."r_name", "nation"."n_name", ANY_VALUE("t"."p_name") AS "PART_NAME", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%BRASS%') AS "t" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1994-01-01' AND "o_orderdate" <= DATE '1994-12-31') AS "t0" INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey") ON "t"."p_partkey" = "lineitem"."l_partkey") ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
GROUP BY "t"."p_name", "region"."r_name", "nation"."n_name"