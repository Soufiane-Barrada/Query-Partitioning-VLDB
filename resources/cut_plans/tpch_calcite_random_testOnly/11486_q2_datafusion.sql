SELECT COALESCE("t2"."p_partkey", "t2"."p_partkey") AS "p_partkey", "t2"."p_name", "t2"."TOTAL_AVAIL_QTY", "t2"."TOTAL_SUPPLY_COST"
FROM (SELECT "s1"."p_partkey", "s1"."p_name", SUM("s1"."ps_availqty") AS "TOTAL_AVAIL_QTY", SUM("s1"."ps_supplycost") AS "TOTAL_SUPPLY_COST"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_partkey", "s1"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t2"