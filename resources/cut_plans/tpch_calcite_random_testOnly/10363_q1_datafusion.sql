SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "part"."p_type" AS "P_TYPE", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_COST"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_brand", "part"."p_type"