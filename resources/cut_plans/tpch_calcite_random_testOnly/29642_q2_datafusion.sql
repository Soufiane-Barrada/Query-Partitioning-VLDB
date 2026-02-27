SELECT COALESCE("t6"."P_NAME", "t6"."P_NAME") AS "P_NAME", "t6"."S_NAME", "t6"."N_NAME", "t6"."R_NAME", "t6"."TOTAL_ORDERS", "t6"."TOTAL_REVENUE", "t6"."AVG_SUPPLIER_BALANCE", "t6"."ORDER_PRIORITIES"
FROM (SELECT "t2"."p_name" AS "P_NAME", "supplier"."s_name" AS "S_NAME", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME", COUNT(DISTINCT "t1"."o_orderkey") AS "TOTAL_ORDERS", SUM("s1"."l_extendedprice") AS "TOTAL_REVENUE", AVG("supplier"."s_acctbal") AS "AVG_SUPPLIER_BALANCE", LISTAGG(DISTINCT "t1"."o_orderpriority", ', ') AS "ORDER_PRIORITIES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t1"
INNER JOIN "s1" ON "t1"."o_orderkey" = "s1"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%widget%') AS "t2" INNER JOIN "TPCH"."partsupp" ON "t2"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "s1"."l_partkey" = "t2"."p_partkey"
GROUP BY "t2"."p_name", "supplier"."s_name", "nation"."n_name", "region"."r_name"
HAVING SUM("s1"."l_extendedprice") > 10000.00
ORDER BY 6 DESC NULLS FIRST, 5 DESC NULLS FIRST) AS "t6"