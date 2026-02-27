SELECT COALESCE("t6"."P_PARTKEY", "t6"."P_PARTKEY") AS "P_PARTKEY", "t6"."P_NAME", "t6"."SUPPLIER_NAME", "t6"."COMBINED_STRING", "t6"."STRING_LENGTH", "t6"."COMMENT_SNIPPET", "t6"."AVG_STRING_LENGTH", "t6"."MAX_STRING_LENGTH", "t6"."TOTAL_ENTRIES", "t6"."UNIQUE_COMMENTS"
FROM (SELECT "t4"."P_PARTKEY", "t4"."P_NAME", "t4"."SUPPLIER_NAME", "t4"."COMBINED_STRING", "t4"."STRING_LENGTH", "t4"."COMMENT_SNIPPET", "s1"."AVG_STRING_LENGTH", "s1"."MAX_STRING_LENGTH", "s1"."TOTAL_ENTRIES", "s1"."UNIQUE_COMMENTS"
FROM "s1",
(SELECT "t3"."p_partkey" AS "P_PARTKEY", "t3"."p_name" AS "P_NAME", "supplier0"."s_name" AS "SUPPLIER_NAME", CONCAT("t3"."p_name", ' - ', "supplier0"."s_name") AS "COMBINED_STRING", LENGTH(CONCAT("t3"."p_name", ' - ', "supplier0"."s_name")) AS "STRING_LENGTH", SUBSTRING("t3"."p_comment", 1, 10) AS "COMMENT_SNIPPET", REGEXP_REPLACE("t3"."p_comment", '[^a-zA-Z0-9]', '') AS "CLEANED_COMMENT"
FROM "TPCH"."supplier" AS "supplier0"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t3" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "t3"."p_partkey" = "partsupp0"."ps_partkey") ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey") AS "t4"
ORDER BY "t4"."STRING_LENGTH" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"