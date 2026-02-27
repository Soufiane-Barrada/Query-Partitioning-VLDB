SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "S_NAME", "TOTAL_QUANTITY", "AVG_PRICE", "ORDER_COUNT", "REGION_NAME"
FROM (SELECT "p_name" AS "P_NAME", "s_name" AS "S_NAME", SUM("l_quantity") AS "TOTAL_QUANTITY", AVG("l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "o_orderkey") AS "ORDER_COUNT", ANY_VALUE("r_name") AS "REGION_NAME"
FROM "s1"
WHERE "p_comment" LIKE '%special%' AND ("l_shipmode" = 'AIR' OR "l_shipmode" = 'RAIL') AND "o_orderdate" >= '1997-01-01' AND "o_orderdate" <= '1997-12-31'
GROUP BY "p_name", "s_name", "r_name"
HAVING SUM("l_quantity") > 100.00
ORDER BY 3 DESC NULLS FIRST, 4) AS "t5"