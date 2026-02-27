SELECT COALESCE("P_PARTKEY", "P_PARTKEY") AS "P_PARTKEY", "P_NAME", "S_NAME", "REVENUE"
FROM (SELECT "p_partkey" AS "P_PARTKEY", "p_name" AS "P_NAME", "s_name" AS "S_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE"
FROM "s1"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31'
GROUP BY "p_partkey", "p_name", "s_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"