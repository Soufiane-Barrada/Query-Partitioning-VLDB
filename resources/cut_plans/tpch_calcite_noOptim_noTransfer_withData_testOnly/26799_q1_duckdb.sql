SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "supplier"."s_name" AS "S_NAME", "nation"."n_name" AS "N_NAME", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", AVG("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "AVG_ORDER_VALUE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "nation"."n_name" LIKE 'A%' AND "lineitem"."l_shipdate" >= DATE '1996-01-01' AND "lineitem"."l_shipdate" <= DATE '1996-12-31' AND "partsupp"."ps_availqty" > 10
GROUP BY "part"."p_name", "supplier"."s_name", "nation"."n_name"