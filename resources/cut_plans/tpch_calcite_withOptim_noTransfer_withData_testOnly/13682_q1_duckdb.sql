SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", "t"."l_extendedprice" * (1 - "t"."l_discount") AS "FD_COL_1"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1995-12-31') AS "t" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t"."l_partkey" = "part"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"