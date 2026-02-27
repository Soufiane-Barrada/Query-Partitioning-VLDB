SELECT COALESCE("t11"."DETAILS", "t11"."DETAILS") AS "DETAILS", "t11"."TOTAL_COUNT", "t11"."TOTAL_SUPPLY_COST"
FROM (SELECT CONCAT('Part: ', "s1"."P_NAME", ', Supplier: ', "t7"."S_NAME", ', Available Qty: ', CAST("partsupp"."ps_availqty" AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "DETAILS", COUNT(*) AS "TOTAL_COUNT", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLY_COST"
FROM "TPCH"."partsupp"
INNER JOIN "s1" ON "partsupp"."ps_partkey" = "s1"."P_PARTKEY"
INNER JOIN (SELECT "s_suppkey" AS "S_SUPPKEY", "s_name" AS "S_NAME", "s_address" AS "S_ADDRESS", "s_phone" AS "S_PHONE", "s_acctbal" AS "S_ACCTBAL", "s_comment" AS "S_COMMENT", SUBSTRING("s_comment", 1, 50) AS "SHORT_COMMENT"
FROM "TPCH"."supplier"
WHERE "s_acctbal" > (((SELECT AVG("s_acctbal")
FROM "TPCH"."supplier")))) AS "t7" ON "partsupp"."ps_suppkey" = "t7"."S_SUPPKEY"
GROUP BY CONCAT('Part: ', "s1"."P_NAME", ', Supplier: ', "t7"."S_NAME", ', Available Qty: ', CAST("partsupp"."ps_availqty" AS VARCHAR CHARACTER SET "ISO-8859-1"))
HAVING COUNT(*) > 1
ORDER BY 3 DESC NULLS FIRST) AS "t11"