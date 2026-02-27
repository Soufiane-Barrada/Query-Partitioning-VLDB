SELECT COALESCE("t"."s_name", "t"."s_name") AS "S_NAME", "t"."s_address" AS "S_ADDRESS", "t"."s_comment", ANY_VALUE(SUBSTRING("t"."s_comment", 1, 25)) AS "COMMENT_EXCERPT", ANY_VALUE(CONCAT('Supplier: ', "t"."s_name", ', Address: ', "t"."s_address")) AS "SUPPLIER_INFO", COUNT(DISTINCT "partsupp"."ps_partkey") AS "PART_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABILITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", ANY_VALUE(REGEXP_REPLACE("t"."s_comment", '[^a-zA-Z0-9 ]', '')) AS "SANITIZED_COMMENT"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%high quality%') AS "t"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t"."s_name", "t"."s_address", "t"."s_comment"