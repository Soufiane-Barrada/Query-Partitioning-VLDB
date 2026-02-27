SELECT COALESCE("CUSTOMER_NAME", "CUSTOMER_NAME") AS "CUSTOMER_NAME", "SUPPLIER_NAME", "PART_NAME", "NUMBER_OF_ORDERS", "TOTAL_REVENUE", "ORDER_SUMMARY"
FROM (SELECT "CUSTOMER_NAME", "SUPPLIER_NAME", "PART_NAME", "NUMBER_OF_ORDERS", "TOTAL_REVENUE", "ORDER_SUMMARY"
FROM (SELECT "customer"."c_name", "customer"."c_address", "s1"."s_name", "s1"."p_name", 'ASIA' AS "r_name", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("s1"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("s1"."p_name") AS "PART_NAME", COUNT(*) AS "NUMBER_OF_ORDERS", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE(CONCAT('Customer located at ', "customer"."c_address", ' ordered ', "s1"."p_name", ' from supplier ', "s1"."s_name", ' in region ', "s1"."r_name")) AS "ORDER_SUMMARY"
FROM "s1"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey") ON "lineitem"."l_orderkey" = "t1"."o_orderkey") ON "s1"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "customer"."c_name", "customer"."c_address", "s1"."s_name", "s1"."p_name") AS "t4"
WHERE "t4"."TOTAL_REVENUE" > 1000.0000
ORDER BY "TOTAL_REVENUE" DESC NULLS FIRST) AS "t7"