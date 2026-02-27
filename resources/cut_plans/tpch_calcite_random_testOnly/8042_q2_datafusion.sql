SELECT COALESCE("t5"."S_NAME", "t5"."S_NAME") AS "S_NAME", "t5"."ORDER_COUNT", "t5"."TOTAL_REVENUE", "t5"."P_NAME", "t5"."P_MFGR", "t5"."P_TYPE", "t5"."R_NAME"
FROM (SELECT "t2"."s_name" AS "S_NAME", "t2"."ORDER_COUNT", "t2"."TOTAL_REVENUE", "t3"."p_name" AS "P_NAME", "t3"."p_mfgr" AS "P_MFGR", "t3"."p_type" AS "P_TYPE", "region"."r_name" AS "R_NAME"
FROM (SELECT "s1"."s_suppkey", "s1"."s_name", COUNT(*) AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t0" INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey") ON "s1"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "s1"."s_suppkey", "s1"."s_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" AS "supplier0" ON "nation"."n_nationkey" = "supplier0"."s_nationkey") ON "t2"."s_suppkey" = "supplier0"."s_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t3" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "t3"."p_partkey" = "partsupp0"."ps_partkey") ON "t2"."s_suppkey" = "partsupp0"."ps_suppkey"
ORDER BY "t2"."TOTAL_REVENUE" DESC NULLS FIRST, "t3"."p_name") AS "t5"