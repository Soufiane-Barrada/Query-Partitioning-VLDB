SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "REVENUE"
FROM (SELECT "p_name" AS "P_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "r_name" = 'EUROPE' AND "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1995-02-01'
GROUP BY "p_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"