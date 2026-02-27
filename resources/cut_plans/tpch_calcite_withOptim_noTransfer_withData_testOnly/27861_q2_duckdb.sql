SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "S_NAME", "TOTAL_QUANTITY", "AVG_PRICE", "ORDER_COUNT", "REGION_NAME"
FROM (SELECT "p_name" AS "P_NAME", "s_name" AS "S_NAME", "TOTAL_QUANTITY", "AVG_PRICE", "ORDER_COUNT", "REGION_NAME"
FROM (SELECT "s1"."p_name", "supplier"."s_name", "region"."r_name", SUM("t2"."l_quantity") AS "TOTAL_QUANTITY", AVG("t2"."l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "t1"."o_orderkey") AS "ORDER_COUNT", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'RAIL')) AS "t2" INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "t2"."l_partkey" = "s1"."p_partkey") ON "t1"."o_orderkey" = "t2"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "region"."r_name", "supplier"."s_name", "s1"."p_name") AS "t4"
WHERE "t4"."TOTAL_QUANTITY" > 100.00
ORDER BY "TOTAL_QUANTITY" DESC NULLS FIRST, "AVG_PRICE") AS "t7"