SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", "orders"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", ANY_VALUE(LEFT("part"."p_comment", 10)) AS "SHORT_COMMENT", ANY_VALUE(CONCAT('Supplier ', "supplier"."s_name", ' provides ', "part"."p_name")) AS "SUPPLIER_INFO", "orders"."o_orderdate" AS "o_orderdate_"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
WHERE "part"."p_type" LIKE '%BRASS%' AND "orders"."o_orderdate" >= DATE '1997-01-01' AND "orders"."o_orderdate" < DATE '1997-12-31'
GROUP BY "part"."p_name", "supplier"."s_name", "customer"."c_name", "orders"."o_orderdate", "part"."p_comment"