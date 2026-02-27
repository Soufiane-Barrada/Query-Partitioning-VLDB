SELECT COALESCE("N_NAME", "N_NAME") AS "N_NAME", "TOTAL_REVENUE", "ORDERS_COUNT"
FROM (SELECT "n_name" AS "N_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "o_orderkey") AS "ORDERS_COUNT"
FROM "s1"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01' AND ("l_shipmode" = 'AIR' OR "l_shipmode" = 'TRUCK')
GROUP BY "n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"