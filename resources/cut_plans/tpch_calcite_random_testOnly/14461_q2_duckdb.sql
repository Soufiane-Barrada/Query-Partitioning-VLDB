SELECT COALESCE("t2"."p_partkey", "t2"."p_partkey") AS "p_partkey", "t2"."p_name", "t2"."TOTAL_AVAIL_QTY", "t2"."AVG_SUPPLY_COST"
FROM (SELECT "part"."p_partkey", "part"."p_name", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAIL_QTY", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "s1"
INNER JOIN "TPCH"."nation" ON "s1"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"