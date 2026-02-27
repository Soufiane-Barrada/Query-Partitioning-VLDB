SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "S_NAME", "supplier"."s_address" AS "S_ADDRESS", ANY_VALUE(SUBSTRING("supplier"."s_comment", 1, 25)) AS "COMMENT_EXCERPT", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ', Address: ', "supplier"."s_address")) AS "SUPPLIER_INFO", COUNT(DISTINCT "partsupp"."ps_partkey") AS "PART_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABILITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", ANY_VALUE(REGEXP_REPLACE("supplier"."s_comment", '[^a-zA-Z0-9 ]', '')) AS "SANITIZED_COMMENT", CASE WHEN AVG("partsupp"."ps_supplycost") > 100.00 THEN 'Expensive ' ELSE 'Affordable' END AS "COST_CATEGORY"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
WHERE "supplier"."s_comment" LIKE '%high quality%'
GROUP BY "supplier"."s_name", "supplier"."s_address", "supplier"."s_comment"