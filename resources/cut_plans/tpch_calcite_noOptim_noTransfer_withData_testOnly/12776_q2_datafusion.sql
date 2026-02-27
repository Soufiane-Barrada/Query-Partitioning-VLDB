SELECT COALESCE("REVENUE", "REVENUE") AS "REVENUE", "N_NAME", "O_YEAR"
FROM (SELECT SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE", "n_name" AS "N_NAME", ANY_VALUE(EXTRACT(YEAR FROM "o_orderdate")) AS "O_YEAR"
FROM "s1"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01' AND "p_brand" = 'Brand#22'
GROUP BY "n_name", EXTRACT(YEAR FROM "o_orderdate")
ORDER BY 1 DESC NULLS FIRST) AS "t4"