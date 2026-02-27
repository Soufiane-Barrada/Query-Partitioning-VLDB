SELECT COALESCE("N_NAME", "N_NAME") AS "N_NAME", "TOTAL_REVENUE"
FROM (SELECT "n_name" AS "N_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01'
GROUP BY "n_name"
ORDER BY 2 DESC NULLS FIRST) AS "t3"