SELECT COALESCE("N_NAME", "N_NAME") AS "N_NAME", "REVENUE"
FROM (SELECT "n_name" AS "N_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1995-12-31'
GROUP BY "n_name"
ORDER BY 2 DESC NULLS FIRST) AS "t3"