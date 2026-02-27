SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "TOTAL_SALES"
FROM (SELECT "p_name" AS "P_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_SALES"
FROM "s1"
WHERE "r_name" = 'ASIA' AND "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" < DATE '1996-12-31'
GROUP BY "p_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"