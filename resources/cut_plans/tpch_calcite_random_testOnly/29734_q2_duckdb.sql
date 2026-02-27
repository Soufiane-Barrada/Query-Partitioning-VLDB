SELECT COALESCE("t4"."SUPPLIER_NAME", "t4"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t4"."PART_NAME", "t4"."NUMBER_OF_ORDERS", "t4"."TOTAL_QUANTITY", "t4"."AVERAGE_EXTENDED_PRICE", "t4"."CUSTOMER_NAMES", "t4"."PART_COMMENTS"
FROM (SELECT "supplier"."s_name", "t1"."p_name", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("t1"."p_name") AS "PART_NAME", COUNT(DISTINCT "t0"."o_orderkey") AS "NUMBER_OF_ORDERS", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", AVG("lineitem"."l_extendedprice") AS "AVERAGE_EXTENDED_PRICE", LISTAGG(DISTINCT CONCAT("customer"."c_name", ' from ', "s1"."r_name"), ', ') AS "CUSTOMER_NAMES", LISTAGG(DISTINCT "t1"."p_comment", '; ') AS "PART_COMMENTS"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey" INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%widget%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "t1"."p_partkey") ON "t0"."o_orderkey" = "lineitem"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name", "t1"."p_name"
ORDER BY 5 DESC NULLS FIRST, 6 DESC NULLS FIRST) AS "t4"