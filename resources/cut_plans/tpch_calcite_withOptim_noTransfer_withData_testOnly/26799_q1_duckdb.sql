SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "supplier"."s_name" AS "S_NAME", "t0"."n_name" AS "N_NAME", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", AVG("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "AVG_ORDER_VALUE"
FROM "TPCH"."orders"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1996-12-31') AS "t" ON "orders"."o_orderkey" = "t"."l_orderkey"
INNER JOIN ((SELECT *
FROM "TPCH"."nation"
WHERE "n_name" LIKE 'A%') AS "t0" INNER JOIN "TPCH"."supplier" ON "t0"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN (SELECT *
FROM "TPCH"."partsupp"
WHERE "ps_availqty" > 10) AS "t1" ON "part"."p_partkey" = "t1"."ps_partkey") ON "supplier"."s_suppkey" = "t1"."ps_suppkey") ON "t"."l_partkey" = "part"."p_partkey"
GROUP BY "part"."p_name", "supplier"."s_name", "t0"."n_name"