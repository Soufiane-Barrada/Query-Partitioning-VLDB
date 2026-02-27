SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", "part"."p_name" AS "P_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", "region"."r_name" AS "R_NAME", "nation"."n_name" AS "N_NAME"
FROM "TPCH"."part"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "lineitem"."l_shipdate" >= DATE '1996-01-01' AND "lineitem"."l_shipdate" < DATE '1996-02-01'
GROUP BY "part"."p_partkey", "part"."p_name", "region"."r_name", "nation"."n_name"