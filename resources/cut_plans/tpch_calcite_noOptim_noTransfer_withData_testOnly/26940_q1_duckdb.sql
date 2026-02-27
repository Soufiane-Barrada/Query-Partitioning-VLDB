SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "part"."p_brand" AS "P_BRAND", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ', Address: ', "supplier"."s_address")) AS "SUPPLIER_INFO", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILQTY", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", AVG("orders"."o_totalprice") AS "AVG_ORDER_VALUE", LISTAGG(DISTINCT CONCAT('Customer: ', "customer"."c_name", ', Segment: ', "customer"."c_mktsegment"), '; ') AS "CUSTOMER_DETAILS"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
WHERE "part"."p_type" LIKE '%metal%' AND "orders"."o_orderdate" >= '1997-01-01' AND "orders"."o_orderdate" <= '1997-12-31'
GROUP BY "part"."p_partkey", "part"."p_name", "part"."p_brand", "supplier"."s_name", "supplier"."s_address"