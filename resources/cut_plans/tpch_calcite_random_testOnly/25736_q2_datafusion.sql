SELECT COALESCE("t4"."SUPPLIER_NAME", "t4"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t4"."PART_NAME", "t4"."TOTAL_ORDERS", "t4"."TOTAL_REVENUE", "t4"."AVG_QUANTITY_PER_ORDER", "t4"."CUSTOMER_NAMES"
FROM (SELECT "s1"."s_name", "part"."p_name", ANY_VALUE("s1"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", COUNT(*) AS "TOTAL_ORDERS", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", AVG("t1"."l_quantity") AS "AVG_QUANTITY_PER_ORDER", LISTAGG(DISTINCT "customer"."c_name", ', ') AS "CUSTOMER_NAMES"
FROM "s1"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" INNER JOIN "TPCH"."orders" ON "t1"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "part"."p_partkey" = "t1"."l_partkey"
GROUP BY "s1"."s_name", "part"."p_name"
ORDER BY 6 DESC NULLS FIRST) AS "t4"