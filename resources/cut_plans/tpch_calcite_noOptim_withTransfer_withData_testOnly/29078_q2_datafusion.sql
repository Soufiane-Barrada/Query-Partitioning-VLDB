SELECT COALESCE("P_PARTKEY", "P_PARTKEY") AS "P_PARTKEY", "P_NAME", "P_RETAILPRICE", "COMMENT_LENGTH", "TOTAL_AVAILABLE_QTY", "SUPPLIER_COUNT", "C_CUSTKEY", "C_NAME", "TOTAL_ORDER_VALUE", "ORDER_COUNT", "COMBINED_INFO"
FROM (SELECT "t4"."P_PARTKEY", "t4"."P_NAME", "t4"."P_RETAILPRICE", "t4"."COMMENT_LENGTH", "t4"."TOTAL_AVAILABLE_QTY", "t4"."SUPPLIER_COUNT", "s1"."C_CUSTKEY", "s1"."C_NAME", "s1"."TOTAL_ORDER_VALUE", "s1"."ORDER_COUNT", CONCAT('Part: ', "t4"."P_NAME", ' | Customer: ', "s1"."C_NAME") AS "COMBINED_INFO"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "part"."p_retailprice" AS "P_RETAILPRICE", ANY_VALUE(LENGTH("part"."p_comment")) AS "COMMENT_LENGTH", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QTY", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", COUNT(DISTINCT "partsupp"."ps_suppkey") > 0 AS "$f6"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "part"."p_partkey", "part"."p_name", "part"."p_retailprice", LENGTH("part"."p_comment")) AS "t4"
INNER JOIN "s1" ON "t4"."$f6") AS "t5"
WHERE LENGTH("t5"."COMBINED_INFO") > 50
FETCH NEXT 100 ROWS ONLY