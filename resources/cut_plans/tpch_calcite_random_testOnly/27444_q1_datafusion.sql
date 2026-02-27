SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", COUNT(DISTINCT "t1"."P_PARTKEY") AS "PART_COUNT", MAX(LENGTH("t1"."UPPER_NAME")) AS "MAX_UPPER_NAME_LENGTH", MIN(LENGTH("t1"."LOWER_COMMENT")) AS "MIN_LOWER_COMMENT_LENGTH", MAX(LENGTH("t1"."MODIFIED_COMMENT")) AS "MAX_MODIFIED_COMMENT_LENGTH", LISTAGG("t1"."BRAND_TYPE", '; ') AS "AGGREGATED_BRAND_TYPE"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN (SELECT "t0"."p_partkey" AS "P_PARTKEY", UPPER("t0"."p_name") AS "UPPER_NAME", LOWER("t0"."p_comment") AS "LOWER_COMMENT", SUBSTRING("t0"."p_brand", 1, 5) AS "BRAND_SUBSTRING", CONCAT('Brand: ', "t0"."p_brand", ' | Type: ', "t0"."p_type") AS "BRAND_TYPE", LENGTH("t0"."p_container") AS "CONTAINER_LENGTH", REPLACE("t0"."p_comment", 'good', 'excellent') AS "MODIFIED_COMMENT"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey") AS "t1" ON "supplier"."s_suppkey" = "t1"."P_PARTKEY"
GROUP BY "nation"."n_name"