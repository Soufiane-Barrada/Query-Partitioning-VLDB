SELECT COALESCE("P_PARTKEY", "P_PARTKEY") AS "P_PARTKEY", AVG("NAME_LENGTH") AS "AVG_NAME_LENGTH", AVG("COMMENT_LENGTH") AS "AVG_COMMENT_LENGTH", COUNT(*) AS "TOTAL_RECORDS", LISTAGG("CONCATENATED_STRING", '; ') AS "AGGREGATED_STRINGS"
FROM (SELECT "p_partkey" AS "P_PARTKEY", "p_name" AS "P_NAME", "p_comment" AS "P_COMMENT", LENGTH("p_name") AS "NAME_LENGTH", LENGTH("p_comment") AS "COMMENT_LENGTH", 'Part Name: ' || "p_name" || ' | Comment: ' || "p_comment" AS "CONCATENATED_STRING"
FROM "TPCH"."part"
WHERE "p_size" > 0
UNION ALL
SELECT "partsupp"."ps_partkey" AS "PS_PARTKEY", "supplier"."s_name" AS "P_NAME", "supplier"."s_comment" AS "P_COMMENT", LENGTH("supplier"."s_name") AS "NAME_LENGTH", LENGTH("supplier"."s_comment") AS "COMMENT_LENGTH", 'Supplier Name: ' || "supplier"."s_name" || ' | Comment: ' || "supplier"."s_comment" AS "CONCATENATED_STRING"
FROM "TPCH"."partsupp"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
WHERE "partsupp"."ps_availqty" > 0) AS "t3"
GROUP BY "P_PARTKEY"