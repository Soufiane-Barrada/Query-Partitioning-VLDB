SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "TOTAL_QUANTITY", "AVG_PRICE", "SUMMARY", "REGION_NAME", "NATION_NAME", "SUPPLIER_NAME", "CUSTOMER_NAME"
FROM (SELECT "p_name" AS "P_NAME", SUM("l_quantity") AS "TOTAL_QUANTITY", AVG("l_extendedprice") AS "AVG_PRICE", CONCAT('Total Quantity: ', CAST(SUM("l_quantity") AS CHAR(1) CHARACTER SET "ISO-8859-1"), ', Avg Price: ', CAST(CAST(AVG("l_extendedprice") AS DECIMAL(10, 2)) AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "SUMMARY", ANY_VALUE("r_name") AS "REGION_NAME", ANY_VALUE("n_name") AS "NATION_NAME", ANY_VALUE("s_name") AS "SUPPLIER_NAME", ANY_VALUE("c_name") AS "CUSTOMER_NAME"
FROM "s1"
WHERE "p_name" LIKE '%widget%' AND "l_shipdate" >= '1996-01-01' AND "l_shipdate" <= '1996-12-31'
GROUP BY "p_name", "r_name", "n_name", "s_name", "c_name"
ORDER BY 2 DESC NULLS FIRST, 3
FETCH NEXT 10 ROWS ONLY) AS "t4"