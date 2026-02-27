SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "p_partkey", "part"."p_name", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILQTY", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLYCOST", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"