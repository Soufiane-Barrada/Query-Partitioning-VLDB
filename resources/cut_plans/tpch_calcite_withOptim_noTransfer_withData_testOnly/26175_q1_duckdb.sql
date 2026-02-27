SELECT COALESCE("t1"."C_CUSTKEY", "t1"."C_CUSTKEY") AS "C_CUSTKEY", "t1"."C_NAME", "t1"."TOTAL_ORDERS", "t1"."AVERAGE_ORDER_VALUE", "t1"."$f4" AS "FD_COL_4", "t7"."S_SUPPKEY", "t7"."S_NAME", "t7"."P_NAME", "t7"."TOTAL_PARTS", "t7"."TOTAL_COST", "t7"."R_REGIONKEY", "t7"."R_NAME", "t7"."NATION_COUNT"
FROM (SELECT "customer"."c_custkey" AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", COUNT(*) AS "TOTAL_ORDERS", AVG("orders"."o_totalprice") AS "AVERAGE_ORDER_VALUE", COUNT(*) > 5 AS "$f4"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name"
HAVING COUNT(*) > 5) AS "t1",
(SELECT "t6"."s_suppkey" AS "S_SUPPKEY", "t6"."s_name" AS "S_NAME", "t6"."p_name" AS "P_NAME", "t6"."TOTAL_PARTS", "t6"."TOTAL_COST", "t4"."R_REGIONKEY", "t4"."R_NAME", "t4"."NATION_COUNT"
FROM (SELECT "region"."r_regionkey" AS "R_REGIONKEY", "region"."r_name" AS "R_NAME", COUNT(*) AS "NATION_COUNT", COUNT(*) > 3 AS "$f3"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
GROUP BY "region"."r_regionkey", "region"."r_name"
HAVING COUNT(*) > 3) AS "t4",
(SELECT "supplier"."s_suppkey", "supplier"."s_name", "part"."p_name", COUNT(*) AS "TOTAL_PARTS", SUM("partsupp"."ps_supplycost") AS "TOTAL_COST"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name", "part"."p_name"
HAVING COUNT(*) > 10) AS "t6") AS "t7"
WHERE "t7"."TOTAL_COST" > 5000.00