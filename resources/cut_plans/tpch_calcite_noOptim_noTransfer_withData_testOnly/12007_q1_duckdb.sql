SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "part"."p_type" AS "P_TYPE", "lineitem"."l_quantity", "lineitem"."l_extendedprice"
FROM "TPCH"."part"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "region"."r_name" = 'ASIA'