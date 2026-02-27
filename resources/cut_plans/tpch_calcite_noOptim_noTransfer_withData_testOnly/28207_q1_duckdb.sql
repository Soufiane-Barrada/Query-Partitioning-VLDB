SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "SUPPLIER_INFO", "SANITIZED_COMMENT", "NAME_LENGTH", "SHORT_ADDRESS", "FORMATTED_PHONE", "ORDER_COUNT"
FROM (SELECT "part"."p_name" AS "P_NAME", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ' | Comment: ', "partsupp"."ps_comment")) AS "SUPPLIER_INFO", ANY_VALUE(REPLACE(REPLACE(LOWER("part"."p_comment"), ' ', '_'), '!', '')) AS "SANITIZED_COMMENT", ANY_VALUE(LENGTH("part"."p_name")) AS "NAME_LENGTH", ANY_VALUE(SUBSTRING("supplier"."s_address", 1, 20)) AS "SHORT_ADDRESS", ANY_VALUE(TRIM(BOTH ' ' FROM "supplier"."s_phone")) AS "FORMATTED_PHONE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
LEFT JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
LEFT JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
GROUP BY "part"."p_name", "supplier"."s_name", "partsupp"."ps_comment", "part"."p_comment", "supplier"."s_address", "supplier"."s_phone") AS "t1"
WHERE "t1"."ORDER_COUNT" > 0