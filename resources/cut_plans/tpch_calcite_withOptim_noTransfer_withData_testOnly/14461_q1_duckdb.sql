SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "p_partkey", "part"."p_name", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAIL_QTY", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'AMERICA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"