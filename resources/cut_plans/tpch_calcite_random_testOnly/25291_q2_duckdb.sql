SELECT COALESCE("t7"."DETAILS", "t7"."DETAILS") AS "DETAILS", "t7"."TOTAL_COUNT", "t7"."TOTAL_SUPPLY_COST"
FROM (SELECT CONCAT('Part: ', "t1"."P_NAME", ', Supplier: ', "t3"."S_NAME", ', Available Qty: ', CAST("partsupp"."ps_availqty" AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "DETAILS", COUNT(*) AS "TOTAL_COUNT", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLY_COST"
FROM (SELECT "part0"."p_partkey" AS "P_PARTKEY", "part0"."p_name" AS "P_NAME", "part0"."p_mfgr" AS "P_MFGR", "part0"."p_brand" AS "P_BRAND", "part0"."p_type" AS "P_TYPE", "part0"."p_size" AS "P_SIZE", "part0"."p_container" AS "P_CONTAINER", "part0"."p_retailprice" AS "P_RETAILPRICE", "part0"."p_comment" AS "P_COMMENT", ROW_NUMBER() OVER (PARTITION BY "part0"."p_brand" ORDER BY "part0"."p_retailprice" DESC NULLS FIRST) AS "PRICE_RANK"
FROM "s1"
INNER JOIN "TPCH"."part" AS "part0" ON "s1"."FD_COL_0" = "part0"."p_size") AS "t1"
INNER JOIN ((SELECT "supplier0"."s_suppkey" AS "S_SUPPKEY", "supplier0"."s_name" AS "S_NAME", "supplier0"."s_address" AS "S_ADDRESS", "supplier0"."s_phone" AS "S_PHONE", "supplier0"."s_acctbal" AS "S_ACCTBAL", "supplier0"."s_comment" AS "S_COMMENT", SUBSTRING("supplier0"."s_comment", 1, 50) AS "SHORT_COMMENT"
FROM (SELECT AVG("s_acctbal") AS "EXPR$0"
FROM "TPCH"."supplier") AS "t2"
INNER JOIN "TPCH"."supplier" AS "supplier0" ON "t2"."EXPR$0" < "supplier0"."s_acctbal") AS "t3" INNER JOIN "TPCH"."partsupp" ON "t3"."S_SUPPKEY" = "partsupp"."ps_suppkey") ON "t1"."P_PARTKEY" = "partsupp"."ps_partkey"
GROUP BY CONCAT('Part: ', "t1"."P_NAME", ', Supplier: ', "t3"."S_NAME", ', Available Qty: ', CAST("partsupp"."ps_availqty" AS VARCHAR CHARACTER SET "ISO-8859-1"))
HAVING COUNT(*) > 1
ORDER BY 3 DESC NULLS FIRST) AS "t7"