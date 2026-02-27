SELECT COALESCE(ANY_VALUE("supplier"."s_name"), ANY_VALUE("supplier"."s_name")) AS "SUPPLIER_NAME", ANY_VALUE("t1"."p_name") AS "PART_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT "customer"."c_name", ', ') AS "CUSTOMER_NAMES", "t1"."p_retailprice" * SUM("partsupp"."ps_availqty") AS "TOTAL_RETAIL_VALUE"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t" ON "customer"."c_custkey" = "t"."o_custkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'widget%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."l_partkey" = "t1"."p_partkey") ON "t"."o_orderkey" = "t0"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name", "t1"."p_name", "t1"."p_retailprice"