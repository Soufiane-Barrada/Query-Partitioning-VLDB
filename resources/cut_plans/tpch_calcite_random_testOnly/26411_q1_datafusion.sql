SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", "supplier"."s_nationkey" AS "S_NATIONKEY", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "partsupp"."ps_partkey") AS "DISTINCT_PARTS_SUPPLIED", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PART_NAMES"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_nationkey"