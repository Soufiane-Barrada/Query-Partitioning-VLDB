SELECT COALESCE("t1"."AVG_STRING_LENGTH", "t1"."AVG_STRING_LENGTH") AS "AVG_STRING_LENGTH", "t1"."MAX_STRING_LENGTH", "t1"."TOTAL_ENTRIES", "t1"."UNIQUE_COMMENTS", "t3"."P_PARTKEY", "t3"."P_NAME", "t3"."SUPPLIER_NAME", "t3"."COMBINED_STRING", "t3"."STRING_LENGTH", "t3"."COMMENT_SNIPPET", "t3"."CLEANED_COMMENT"
FROM (SELECT AVG(LENGTH(CONCAT("t"."p_name", ' - ', "supplier"."s_name"))) AS "AVG_STRING_LENGTH", MAX(LENGTH(CONCAT("t"."p_name", ' - ', "supplier"."s_name"))) AS "MAX_STRING_LENGTH", COUNT(*) AS "TOTAL_ENTRIES", COUNT(DISTINCT REGEXP_REPLACE("t"."p_comment", '[^a-zA-Z0-9]', '')) AS "UNIQUE_COMMENTS"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") AS "t1",
(SELECT "t2"."p_partkey" AS "P_PARTKEY", "t2"."p_name" AS "P_NAME", "supplier0"."s_name" AS "SUPPLIER_NAME", CONCAT("t2"."p_name", ' - ', "supplier0"."s_name") AS "COMBINED_STRING", LENGTH(CONCAT("t2"."p_name", ' - ', "supplier0"."s_name")) AS "STRING_LENGTH", SUBSTRING("t2"."p_comment", 1, 10) AS "COMMENT_SNIPPET", REGEXP_REPLACE("t2"."p_comment", '[^a-zA-Z0-9]', '') AS "CLEANED_COMMENT"
FROM "TPCH"."supplier" AS "supplier0"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t2" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "t2"."p_partkey" = "partsupp0"."ps_partkey") ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey") AS "t3"