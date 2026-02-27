SELECT COALESCE("customer"."c_name", "customer"."c_name") AS "c_name", "customer"."c_address", "supplier"."s_name", "part"."p_name", 'ASIA' AS "r_name", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", COUNT(*) AS "NUMBER_OF_ORDERS", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE(CONCAT('Customer located at ', "customer"."c_address", ' ordered ', "part"."p_name", ' from supplier ', "supplier"."s_name", ' in region ', "t"."r_name")) AS "ORDER_SUMMARY"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey") ON "lineitem"."l_orderkey" = "t0"."o_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "customer"."c_name", "customer"."c_address", "supplier"."s_name", "part"."p_name"