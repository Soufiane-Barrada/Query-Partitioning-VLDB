SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "REVENUE"
FROM (SELECT "p_name" AS "P_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "r_name" = 'ASIA' AND "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1996-01-01'
GROUP BY "p_name"
ORDER BY 2 DESC NULLS FIRST) AS "t3"