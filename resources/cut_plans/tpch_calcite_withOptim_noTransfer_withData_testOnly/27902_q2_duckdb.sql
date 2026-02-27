SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."S_NAME", "t5"."CUSTOMER_COUNT", "t5"."ORDER_COUNT", "t5"."TOTAL_QUANTITY", "t5"."AVG_PRICE", "t5"."REGIONS_SERVED"
FROM (SELECT "part"."p_name" AS "P_NAME", "supplier"."s_name" AS "S_NAME", "t2"."CUSTOMER_COUNT", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", AVG("lineitem"."l_extendedprice") AS "AVG_PRICE", LISTAGG(DISTINCT "s1"."r_name", ', ') AS "REGIONS_SERVED"
FROM (SELECT COUNT(DISTINCT "c_custkey") AS "CUSTOMER_COUNT"
FROM "TPCH"."customer"
WHERE "c_acctbal" > 1000.00) AS "t2"
CROSS JOIN ("TPCH"."supplier" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey" INNER JOIN ("TPCH"."lineitem" INNER JOIN ("s1" INNER JOIN "TPCH"."nation" ON "s1"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."customer" AS "customer0" ON "nation"."n_nationkey" = "customer0"."c_nationkey" INNER JOIN "TPCH"."orders" ON "customer0"."c_custkey" = "orders"."o_custkey") ON "lineitem"."l_orderkey" = "orders"."o_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey")
GROUP BY "part"."p_name", "supplier"."s_name", "t2"."CUSTOMER_COUNT"
ORDER BY 5 DESC NULLS FIRST, 6 DESC NULLS FIRST) AS "t5"