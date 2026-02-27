SELECT COALESCE("P_PARTKEY", "P_PARTKEY") AS "P_PARTKEY", "P_NAME", "REVENUE"
FROM (SELECT "p_partkey" AS "P_PARTKEY", "p_name" AS "P_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "r_name" = 'EUROPE' AND "l_shipdate" >= '1997-01-01' AND "l_shipdate" < '1997-12-31'
GROUP BY "p_partkey", "p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"