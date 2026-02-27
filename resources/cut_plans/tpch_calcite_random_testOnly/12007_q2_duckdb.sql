SELECT COALESCE("p_brand", "p_brand") AS "p_brand", "p_type", "TOTAL_QUANTITY", "AVG_PRICE"
FROM (SELECT "p_brand", "p_type", SUM("l_quantity") AS "TOTAL_QUANTITY", AVG("l_extendedprice") AS "AVG_PRICE"
FROM "s1"
GROUP BY "p_brand", "p_type"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t2"