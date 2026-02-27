SELECT COALESCE("CUSTOMER_NAME", "CUSTOMER_NAME") AS "CUSTOMER_NAME", "SUPPLIER_NAME", "PART_NAME", "NUMBER_OF_ORDERS", "TOTAL_REVENUE", "ORDER_SUMMARY"
FROM (SELECT ANY_VALUE("c_name") AS "CUSTOMER_NAME", ANY_VALUE("s_name") AS "SUPPLIER_NAME", ANY_VALUE("p_name") AS "PART_NAME", COUNT(*) AS "NUMBER_OF_ORDERS", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE", ANY_VALUE(CONCAT('Customer located at ', "c_address", ' ordered ', "p_name", ' from supplier ', "s_name", ' in region ', "r_name")) AS "ORDER_SUMMARY"
FROM "s1"
WHERE "r_name" = 'ASIA' AND "o_orderdate" >= '1997-01-01' AND "o_orderdate" <= '1997-12-31'
GROUP BY "c_name", "c_address", "s_name", "p_name", "r_name"
HAVING SUM("l_extendedprice" * (1 - "l_discount")) > 1000.0000
ORDER BY 5 DESC NULLS FIRST) AS "t5"