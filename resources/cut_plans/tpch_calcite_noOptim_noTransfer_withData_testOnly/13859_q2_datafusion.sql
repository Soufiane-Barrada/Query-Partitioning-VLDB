SELECT COALESCE("P_PARTKEY", "P_PARTKEY") AS "P_PARTKEY", "REVENUE"
FROM (SELECT "p_partkey" AS "P_PARTKEY", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "r_name" = 'ASIA' AND "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" < DATE '1997-01-01'
GROUP BY "p_partkey"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"