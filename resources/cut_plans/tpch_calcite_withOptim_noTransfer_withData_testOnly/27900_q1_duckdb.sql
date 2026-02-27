SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "s_name", "nation"."n_name", "region"."r_name", CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ' in ', "region"."r_name") AS "FD_COL_3", "orders"."o_orderkey", "t"."l_extendedprice" * (1 - "t"."l_discount") AS "FD_COL_5", CASE WHEN "t"."l_discount" > 0.00 THEN CAST("t"."l_extendedprice" AS DECIMAL(15, 2)) ELSE NULL END AS "FD_COL_6", "part"."p_name", ', ' AS "FD_COL_8", CASE WHEN "customer"."c_mktsegment" = 'FURNITURE' THEN "customer"."c_custkey" ELSE NULL END AS "FD_COL_9"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t" INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "part"."p_partkey" = "t"."l_partkey"