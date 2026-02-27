SELECT COALESCE("P_BRAND", "P_BRAND") AS "P_BRAND", "P_TYPE", "REVENUE"
FROM (SELECT "p_brand" AS "P_BRAND", "p_type" AS "P_TYPE", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "r_name" = 'ASIA' AND "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1995-02-01'
GROUP BY "p_brand", "p_type"
ORDER BY 3 DESC NULLS FIRST) AS "t3"