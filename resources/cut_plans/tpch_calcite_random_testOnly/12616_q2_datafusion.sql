SELECT COALESCE("t1"."p_partkey", "t1"."p_partkey") AS "p_partkey", "t1"."p_name", "t1"."TOTAL_AVAILQTY", "t1"."AVG_SUPPLYCOST", "t1"."SUPPLIER_COUNT"
FROM (SELECT "s1"."p_partkey", "s1"."p_name", SUM("s1"."ps_availqty") AS "TOTAL_AVAILQTY", AVG("s1"."ps_supplycost") AS "AVG_SUPPLYCOST", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT"
FROM "TPCH"."supplier"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_partkey", "s1"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t1"