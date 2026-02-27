SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "S_NAME", "TOTAL_QUANTITY", "AVG_PRICE", "ORDER_COUNT", "REGION_NAME"
FROM (SELECT "p_name" AS "P_NAME", "s_name" AS "S_NAME", "TOTAL_QUANTITY", "AVG_PRICE", "ORDER_COUNT", "REGION_NAME"
FROM (SELECT "t2"."p_name", "supplier"."s_name", "s1"."r_name", SUM("t1"."l_quantity") AS "TOTAL_QUANTITY", AVG("t1"."l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDER_COUNT", ANY_VALUE("s1"."r_name") AS "REGION_NAME"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'RAIL')) AS "t1" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_comment" LIKE '%special%') AS "t2" INNER JOIN "TPCH"."partsupp" ON "t2"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."l_partkey" = "t2"."p_partkey") ON "t0"."o_orderkey" = "t1"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."r_name", "supplier"."s_name", "t2"."p_name") AS "t4"
WHERE "t4"."TOTAL_QUANTITY" > 100.00
ORDER BY "TOTAL_QUANTITY" DESC NULLS FIRST, "AVG_PRICE") AS "t7"