SELECT COALESCE("S_NAME", "S_NAME") AS "S_NAME", "TOTAL_COST"
FROM (SELECT "s_name" AS "S_NAME", SUM("ps_supplycost" * "ps_availqty") AS "TOTAL_COST"
FROM "s1"
WHERE "o_orderdate" >= '1997-01-01' AND "o_orderdate" < '1997-12-31'
GROUP BY "s_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"