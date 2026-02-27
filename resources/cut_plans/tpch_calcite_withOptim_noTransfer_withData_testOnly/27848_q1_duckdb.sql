SELECT COALESCE("t3"."NATION_NAME", "t3"."NATION_NAME") AS "NATION_NAME", "t3"."PART_SUPPLIER_LIST", "t3"."PART_COUNT", "t5"."TOTAL_PART_COUNT", ROUND("t3"."PART_COUNT" * 100.0 / "t5"."TOTAL_PART_COUNT", 2) AS "PERCENTAGE_OF_TOTAL"
FROM (SELECT ANY_VALUE("nation"."n_name") AS "NATION_NAME", LISTAGG(CONCAT("t"."p_name", ' (', "supplier"."s_name", ')'), ', ') AS "PART_SUPPLIER_LIST", COUNT(DISTINCT "t"."p_partkey") AS "PART_COUNT"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE LENGTH("p_name") > 15) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "nation"."n_name"
HAVING COUNT(DISTINCT "t"."p_partkey") > 0) AS "t3",
(SELECT COUNT(DISTINCT "p_partkey") AS "TOTAL_PART_COUNT"
FROM "TPCH"."part"
WHERE LENGTH("p_name") > 15) AS "t5"