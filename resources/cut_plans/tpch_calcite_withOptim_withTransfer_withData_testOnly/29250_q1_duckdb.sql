SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "P_NAME", "supplier"."s_name", "customer"."c_name", "t"."o_orderdate" AS "O_ORDERDATE", "t0"."p_comment", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", ANY_VALUE(LEFT("t0"."p_comment", 10)) AS "SHORT_COMMENT", ANY_VALUE(CONCAT('Supplier ', "supplier"."s_name", ' provides ', "t0"."p_name")) AS "SUPPLIER_INFO"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey" INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%BRASS%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "t0"."p_partkey") ON "t"."o_orderkey" = "lineitem"."l_orderkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_name", "supplier"."s_name", "customer"."c_name", "t"."o_orderdate", "t0"."p_comment"