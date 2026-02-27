SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "P_NAME", "partsupp"."ps_suppkey", "partsupp"."ps_supplycost", CASE WHEN "lineitem"."l_discount" > 0.00 THEN "lineitem"."l_quantity" ELSE 0.00 END AS "FD_COL_3", CASE WHEN "orders"."o_orderdate" > DATE '1997-01-01' THEN CAST("orders"."o_totalprice" AS DECIMAL(15, 2)) ELSE NULL END AS "FD_COL_4", "supplier"."s_name", ', ' AS "FD_COL_6"
FROM "TPCH"."orders"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ((SELECT "n_nationkey"
FROM "TPCH"."nation"
WHERE "n_name" = 'Germany'
GROUP BY "n_nationkey") AS "t0" INNER JOIN "TPCH"."supplier" ON "t0"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%raw%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"