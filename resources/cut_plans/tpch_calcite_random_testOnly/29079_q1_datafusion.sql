SELECT COALESCE(AVG(LENGTH(CONCAT("t"."p_name", ' - ', "supplier"."s_name"))), AVG(LENGTH(CONCAT("t"."p_name", ' - ', "supplier"."s_name")))) AS "AVG_STRING_LENGTH", MAX(LENGTH(CONCAT("t"."p_name", ' - ', "supplier"."s_name"))) AS "MAX_STRING_LENGTH", COUNT(*) AS "TOTAL_ENTRIES", COUNT(DISTINCT REGEXP_REPLACE("t"."p_comment", '[^a-zA-Z0-9]', '')) AS "UNIQUE_COMMENTS"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"