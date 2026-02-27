SELECT COALESCE("region"."r_regionkey", "region"."r_regionkey") AS "R_REGIONKEY", "region"."r_name", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2", "customer"."c_custkey", "t"."o_orderkey"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-10-01') AS "t" ON "customer"."c_custkey" = "t"."o_custkey") ON "lineitem"."l_orderkey" = "t"."o_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"