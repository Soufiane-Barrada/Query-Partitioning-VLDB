SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST", SUM(CASE WHEN "lineitem"."l_discount" > 0.00 THEN "lineitem"."l_quantity" ELSE 0.00 END) AS "TOTAL_DISCOUNTED_QUANTITY", MAX(CASE WHEN "orders"."o_orderdate" > DATE '1997-01-01' THEN CAST("orders"."o_totalprice" AS DECIMAL(15, 2)) ELSE NULL END) AS "MAX_RECENT_ORDER_VALUE", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "SUPPLIER_NAMES"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "part"."p_type" LIKE '%raw%' AND "supplier"."s_nationkey" IN (SELECT "n_nationkey" AS "N_NATIONKEY"
FROM "TPCH"."nation"
WHERE "n_name" = 'Germany')
GROUP BY "part"."p_name"
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 5