SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "part"."p_type" AS "P_TYPE", "t0"."l_extendedprice" * (1 - "t0"."l_discount") AS "FD_COL_2"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1996-01-01') AS "t0" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."l_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"