SELECT COALESCE(ANY_VALUE("supplier"."s_name"), ANY_VALUE("supplier"."s_name")) AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT "customer"."c_name", ', ') AS "CUSTOMER_NAMES", "part"."p_retailprice" * SUM("partsupp"."ps_availqty") AS "TOTAL_RETAIL_VALUE"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
WHERE "part"."p_name" LIKE 'widget%' AND "orders"."o_orderstatus" = 'O' AND "lineitem"."l_shipdate" >= DATE '1997-01-01' AND "lineitem"."l_shipdate" <= DATE '1997-12-31'
GROUP BY "supplier"."s_name", "part"."p_name", "part"."p_retailprice"