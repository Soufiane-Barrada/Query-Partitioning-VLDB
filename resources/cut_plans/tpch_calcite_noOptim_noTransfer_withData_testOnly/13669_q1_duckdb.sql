SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", "part"."p_name" AS "P_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "region"."r_name" = 'EUROPE'
GROUP BY "part"."p_partkey", "part"."p_name"