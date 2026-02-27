SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "region"."r_name", "nation"."n_name", "supplier"."s_name", "customer"."c_name", "lineitem"."l_quantity", "lineitem"."l_extendedprice"
FROM "TPCH"."part"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."customer" ON "lineitem"."l_orderkey" = "customer"."c_custkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "part"."p_name" LIKE '%widget%' AND "lineitem"."l_shipdate" >= '1996-01-01' AND "lineitem"."l_shipdate" <= '1996-12-31'