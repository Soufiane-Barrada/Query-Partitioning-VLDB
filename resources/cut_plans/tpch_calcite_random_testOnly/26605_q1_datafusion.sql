SELECT COALESCE("part"."p_name", "part"."p_name") AS "p_name", "supplier"."s_name", "nation"."n_name", "t"."c_name", "t0"."l_quantity", "t0"."l_extendedprice" * (1 - "t0"."l_discount") AS "FD_COL_5", "orders"."o_orderkey"
FROM (SELECT *
FROM "TPCH"."customer"
WHERE "c_mktsegment" = 'BUILDING') AS "t"
INNER JOIN ("TPCH"."part" INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "partsupp"."ps_partkey" = "t0"."l_partkey") ON "part"."p_partkey" = "t0"."l_partkey") ON "t"."c_custkey" = "orders"."o_custkey"