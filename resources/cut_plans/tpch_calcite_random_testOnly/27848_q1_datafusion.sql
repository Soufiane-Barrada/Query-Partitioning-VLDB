SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", ANY_VALUE("nation"."n_name") AS "NATION_NAME", LISTAGG(CONCAT("t"."p_name", ' (', "supplier"."s_name", ')'), ', ') AS "PART_SUPPLIER_LIST", COUNT(DISTINCT "t"."p_partkey") AS "PART_COUNT"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE LENGTH("p_name") > 15) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "nation"."n_name"