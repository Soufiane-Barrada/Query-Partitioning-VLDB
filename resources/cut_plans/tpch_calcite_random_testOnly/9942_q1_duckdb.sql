SELECT COALESCE("t1"."S_SUPPKEY", "t1"."S_SUPPKEY") AS "S_SUPPKEY", "t1"."S_NAME", "t1"."TOTAL_SUPPLY_COST"
FROM (SELECT "supplier"."s_suppkey" AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLY_COST"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t1"