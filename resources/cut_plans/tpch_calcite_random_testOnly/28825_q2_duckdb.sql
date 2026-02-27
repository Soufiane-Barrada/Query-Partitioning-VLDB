SELECT COALESCE("t3"."S_NAME", "t3"."S_NAME") AS "S_NAME", "t3"."S_ADDRESS", "t3"."COMMENT_EXCERPT", "t3"."SUPPLIER_INFO", "t3"."PART_COUNT", "t3"."TOTAL_AVAILABILITY", "t3"."AVERAGE_SUPPLY_COST", "t3"."SANITIZED_COMMENT", CASE WHEN "t3"."AVERAGE_SUPPLY_COST" > 100.00 THEN 'Expensive ' ELSE 'Affordable' END AS "COST_CATEGORY"
FROM (SELECT "t0"."s_name" AS "S_NAME", "t0"."s_address" AS "S_ADDRESS", "t0"."s_comment", ANY_VALUE(SUBSTRING("t0"."s_comment", 1, 25)) AS "COMMENT_EXCERPT", ANY_VALUE(CONCAT('Supplier: ', "t0"."s_name", ', Address: ', "t0"."s_address")) AS "SUPPLIER_INFO", COUNT(DISTINCT "s1"."ps_partkey") AS "PART_COUNT", SUM("s1"."ps_availqty") AS "TOTAL_AVAILABILITY", AVG("s1"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", ANY_VALUE(REGEXP_REPLACE("t0"."s_comment", '[^a-zA-Z0-9 ]', '')) AS "SANITIZED_COMMENT"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%high quality%') AS "t0"
INNER JOIN "s1" ON "t0"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "t0"."s_name", "t0"."s_address", "t0"."s_comment"
ORDER BY 7 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"