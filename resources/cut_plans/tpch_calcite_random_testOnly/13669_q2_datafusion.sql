SELECT COALESCE("t2"."p_partkey", "t2"."p_partkey") AS "p_partkey", "t2"."p_name", "t2"."TOTAL_AVAILABLE_QUANTITY", "t2"."AVERAGE_SUPPLY_COST"
FROM (SELECT "part"."p_partkey", "part"."p_name", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t2"