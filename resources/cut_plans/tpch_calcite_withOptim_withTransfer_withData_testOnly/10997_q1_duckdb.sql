SELECT COALESCE("lineitem"."l_orderkey", "lineitem"."l_orderkey") AS "L_ORDERKEY", "t"."o_orderdate" AS "O_ORDERDATE", "customer"."c_name" AS "C_NAME", "supplier"."s_name" AS "S_NAME", "part"."p_name" AS "P_NAME", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey") ON "lineitem"."l_orderkey" = "t"."o_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "lineitem"."l_orderkey", "t"."o_orderdate", "customer"."c_name", "supplier"."s_name", "part"."p_name", "nation"."n_name", "region"."r_name"