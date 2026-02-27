SELECT COALESCE("t6"."SUPPLIER_NAME", "t6"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t6"."PART_NAME", "t6"."TOTAL_AVAILABLE_QUANTITY", "t6"."TOTAL_ORDERS", "t6"."CUSTOMER_NAMES", "t6"."TOTAL_RETAIL_VALUE"
FROM (SELECT ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("t2"."p_name") AS "PART_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "t1"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT "customer"."c_name", ', ') AS "CUSTOMER_NAMES", "t2"."p_retailprice" * SUM("partsupp"."ps_availqty") AS "TOTAL_RETAIL_VALUE"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey" INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'widget%') AS "t2" INNER JOIN "TPCH"."partsupp" ON "t2"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."l_partkey" = "t2"."p_partkey") ON "t1"."o_orderkey" = "s1"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name", "t2"."p_name", "t2"."p_retailprice"
ORDER BY 6 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"