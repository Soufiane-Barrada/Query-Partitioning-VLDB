SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "region"."r_name" AS "R_NAME", "nation"."n_name" AS "N_NAME", SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "REVENUE"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" < DATE '1996-02-01') AS "t" ON "part"."p_partkey" = "t"."l_partkey") ON "supplier"."s_suppkey" = "t"."l_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name", "region"."r_name", "nation"."n_name"