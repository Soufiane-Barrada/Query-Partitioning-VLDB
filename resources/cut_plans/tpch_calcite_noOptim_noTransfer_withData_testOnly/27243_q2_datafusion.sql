SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "P_BRAND", "TOTAL_AVAILABLE_QUANTITY", "AVERAGE_PRICE", "TOTAL_ORDERS"
FROM (SELECT "p_name" AS "P_NAME", "p_brand" AS "P_BRAND", SUM("ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("p_retailprice") AS "AVERAGE_PRICE", COUNT(DISTINCT "l_orderkey") AS "TOTAL_ORDERS"
FROM "s1"
WHERE "p_size" >= 10 AND "p_size" <= 20 AND "r_name" LIKE 'Europe%' AND "o_orderstatus" = 'O' AND ("l_shipmode" = 'AIR' OR "l_shipmode" = 'TRUCK')
GROUP BY "p_name", "p_brand"
HAVING SUM("ps_availqty") > 500
ORDER BY 4 DESC NULLS FIRST) AS "t4"