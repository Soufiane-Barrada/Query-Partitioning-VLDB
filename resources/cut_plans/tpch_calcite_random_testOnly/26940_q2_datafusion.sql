SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."P_BRAND", "t4"."SUPPLIER_INFO", "t4"."TOTAL_AVAILQTY", "t4"."TOTAL_ORDERS", "t4"."AVG_ORDER_VALUE", "t4"."CUSTOMER_DETAILS"
FROM (SELECT "t1"."p_partkey", "t1"."p_name" AS "P_NAME", "t1"."p_brand" AS "P_BRAND", "supplier"."s_name", "supplier"."s_address", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ', Address: ', "supplier"."s_address")) AS "SUPPLIER_INFO", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILQTY", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS", AVG("s1"."o_totalprice") AS "AVG_ORDER_VALUE", LISTAGG(DISTINCT CONCAT('Customer: ', "s1"."c_name", ', Segment: ', "s1"."c_mktsegment"), '; ') AS "CUSTOMER_DETAILS"
FROM "TPCH"."supplier"
INNER JOIN ("s1" INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "t1"."p_partkey") ON "s1"."o_orderkey" = "lineitem"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t1"."p_partkey", "t1"."p_name", "t1"."p_brand", "supplier"."s_name", "supplier"."s_address"
ORDER BY 7 DESC NULLS FIRST, 9 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"