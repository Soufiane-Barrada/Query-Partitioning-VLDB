SELECT COALESCE("P_BRAND", "P_BRAND") AS "P_BRAND", "P_TYPE", "TOTAL_QUANTITY", "TOTAL_REVENUE"
FROM (SELECT "p_brand" AS "P_BRAND", "p_type" AS "P_TYPE", SUM("l_quantity") AS "TOTAL_QUANTITY", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
WHERE "r_name" = 'Europe' AND "l_shipdate" >= '1995-01-01' AND "l_shipdate" < '1995-12-31'
GROUP BY "p_brand", "p_type"
ORDER BY 4 DESC NULLS FIRST) AS "t3"