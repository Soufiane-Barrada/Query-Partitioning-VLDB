SELECT COALESCE("t4"."p_name", "t4"."p_name") AS "P_NAME", "t4"."TOTAL_QUANTITY", "t4"."AVG_PRICE", CONCAT('Total Quantity: ', CAST("t4"."TOTAL_QUANTITY" AS CHAR(1) CHARACTER SET "ISO-8859-1"), ', Avg Price: ', CAST(CAST("t4"."AVG_PRICE" AS DECIMAL(10, 2)) AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "SUMMARY", "t4"."REGION_NAME", "t4"."NATION_NAME", "t4"."SUPPLIER_NAME", "t4"."CUSTOMER_NAME"
FROM (SELECT "t1"."p_name", "s1"."r_name", "s1"."n_name", "supplier"."s_name", "customer"."c_name", SUM("t0"."l_quantity") AS "TOTAL_QUANTITY", AVG("t0"."l_extendedprice") AS "AVG_PRICE", ANY_VALUE("s1"."r_name") AS "REGION_NAME", ANY_VALUE("s1"."n_name") AS "NATION_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1996-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."l_orderkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%widget%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t0"."l_partkey" = "t1"."p_partkey"
GROUP BY "customer"."c_name", "s1"."r_name", "s1"."n_name", "supplier"."s_name", "t1"."p_name"
ORDER BY 6 DESC NULLS FIRST, 7
FETCH NEXT 10 ROWS ONLY) AS "t4"