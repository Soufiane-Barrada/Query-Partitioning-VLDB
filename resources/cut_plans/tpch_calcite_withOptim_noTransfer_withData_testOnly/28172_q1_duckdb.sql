SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "P_NAME", "t0"."s_name" AS "S_NAME", "nation"."n_name" AS "N_NAME", LEFT("t0"."s_address", POSITION(',' IN "t0"."s_address") - 1) AS "FD_COL_3", ANY_VALUE(LEFT("t0"."s_address", POSITION(',' IN "t0"."s_address") - 1)) AS "CITY", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", AVG("lineitem"."l_extendedprice") AS "AVG_PRICE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."nation" INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 5000.00) AS "t0" ON "nation"."n_nationkey" = "t0"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%steel%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"
GROUP BY "t1"."p_name", "t0"."s_name", "nation"."n_name", LEFT("t0"."s_address", POSITION(',' IN "t0"."s_address") - 1)