SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_1"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01') AS "t" ON "customer"."c_custkey" = "t"."o_custkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey" INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey" AND "partsupp"."ps_suppkey" = "supplier"."s_suppkey") ON "t"."o_orderkey" = "lineitem"."l_orderkey"