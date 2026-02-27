SELECT COALESCE("t0"."p_partkey", "t0"."p_partkey") AS "p_partkey", "t0"."p_name" AS "P_NAME", "t0"."p_brand" AS "P_BRAND", "supplier"."s_name", "supplier"."s_address", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ', Address: ', "supplier"."s_address")) AS "SUPPLIER_INFO", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILQTY", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", AVG("t"."o_totalprice") AS "AVG_ORDER_VALUE", LISTAGG(DISTINCT CONCAT('Customer: ', "customer"."c_name", ', Segment: ', "customer"."c_mktsegment"), '; ') AS "CUSTOMER_DETAILS"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey" INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "t0"."p_partkey") ON "t"."o_orderkey" = "lineitem"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_partkey", "t0"."p_name", "t0"."p_brand", "supplier"."s_name", "supplier"."s_address"