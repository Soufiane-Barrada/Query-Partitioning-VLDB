SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "P_NAME", "supplier"."s_name" AS "S_NAME", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", SUM("t0"."l_extendedprice") AS "TOTAL_REVENUE", AVG("supplier"."s_acctbal") AS "AVG_SUPPLIER_BALANCE", LISTAGG(DISTINCT "t"."o_orderpriority", ', ') AS "ORDER_PRIORITIES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01') AS "t0" ON "t"."o_orderkey" = "t0"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%widget%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t0"."l_partkey" = "t1"."p_partkey"
GROUP BY "t1"."p_name", "supplier"."s_name", "nation"."n_name", "region"."r_name"