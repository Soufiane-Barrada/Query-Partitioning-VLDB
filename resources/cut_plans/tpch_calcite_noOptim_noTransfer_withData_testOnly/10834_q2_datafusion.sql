SELECT COALESCE("P_BRAND", "P_BRAND") AS "P_BRAND", "P_TYPE", "TOTAL_QUANTITY", "TOTAL_SALES"
FROM (SELECT "p_brand" AS "P_BRAND", "p_type" AS "P_TYPE", SUM("l_quantity") AS "TOTAL_QUANTITY", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_SALES"
FROM "s1"
WHERE "r_name" = 'ASIA' AND "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" < DATE '1996-01-31'
GROUP BY "p_brand", "p_type"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"