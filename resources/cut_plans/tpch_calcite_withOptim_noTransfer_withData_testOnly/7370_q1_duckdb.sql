SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "region"."r_name", "t"."l_extendedprice" * (1 - "t"."l_discount") AS "FD_COL_2", "t0"."o_orderkey", "customer"."c_custkey"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'TRUCK')) AS "t" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-10-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey") ON "t"."l_orderkey" = "t0"."o_orderkey") ON "part"."p_partkey" = "t"."l_partkey"