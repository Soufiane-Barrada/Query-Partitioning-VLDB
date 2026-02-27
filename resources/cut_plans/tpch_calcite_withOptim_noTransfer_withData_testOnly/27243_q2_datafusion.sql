SELECT COALESCE("p_name", "p_name") AS "p_name", "p_brand", "TOTAL_AVAILABLE_QUANTITY", "AVERAGE_PRICE", "TOTAL_ORDERS"
FROM (SELECT "p_name", "p_brand", SUM("ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("p_retailprice") AS "AVERAGE_PRICE", COUNT(DISTINCT "l_orderkey") AS "TOTAL_ORDERS"
FROM "s1"
GROUP BY "p_name", "p_brand"
HAVING SUM("ps_availqty") > 500
ORDER BY 4 DESC NULLS FIRST) AS "t6"