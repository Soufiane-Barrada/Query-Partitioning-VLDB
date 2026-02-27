SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SALES"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey") ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
GROUP BY "part"."p_name"