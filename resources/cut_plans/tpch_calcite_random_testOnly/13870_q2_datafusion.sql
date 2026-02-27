SELECT COALESCE("p_partkey", "p_partkey") AS "p_partkey", "p_name", "TOTAL_QUANTITY", "TOTAL_SALES"
FROM (SELECT "p_partkey", "p_name", SUM("l_quantity") AS "TOTAL_QUANTITY", SUM("l_extendedprice") AS "TOTAL_SALES"
FROM "s1"
GROUP BY "p_partkey", "p_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"