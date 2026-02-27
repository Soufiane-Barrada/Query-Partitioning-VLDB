SELECT COALESCE("t5"."PART_DETAILS", "t5"."PART_DETAILS") AS "PART_DETAILS", "t5"."SUPPLIER_NAME", "t5"."CUSTOMER_INFO", "t5"."O_ORDERKEY", "t5"."O_ORDERDATE", "t5"."TOTAL_REVENUE"
FROM (SELECT "t1"."p_name", "t1"."p_brand", "t1"."p_type", "t1"."p_container", "t1"."p_comment", "supplier"."s_name", "s1"."c_name", "s1"."c_address", "s1"."o_orderkey" AS "O_ORDERKEY", "s1"."o_orderdate" AS "O_ORDERDATE", ANY_VALUE(CONCAT('Part Name: ', "t1"."p_name", ', Brand: ', "t1"."p_brand", ', Type: ', "t1"."p_type", ', Container: ', "t1"."p_container", ', Comment: ', "t1"."p_comment")) AS "PART_DETAILS", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE(CONCAT("s1"."c_name", ' (', "s1"."c_address", ')')) AS "CUSTOMER_INFO", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN "s1" ON "lineitem"."l_orderkey" = "s1"."o_orderkey") ON "t1"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "t1"."p_name", "t1"."p_brand", "t1"."p_type", "t1"."p_container", "t1"."p_comment", "supplier"."s_name", "s1"."c_name", "s1"."c_address", "s1"."o_orderkey", "s1"."o_orderdate"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 5000.0000
ORDER BY 14 DESC NULLS FIRST) AS "t5"