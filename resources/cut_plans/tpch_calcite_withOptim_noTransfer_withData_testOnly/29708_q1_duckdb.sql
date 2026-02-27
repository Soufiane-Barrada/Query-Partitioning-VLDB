SELECT COALESCE("t"."p_name", "t"."p_name") AS "p_name", "t"."p_brand", "t"."p_type", "t"."p_container", "t"."p_comment", "supplier"."s_name", "customer"."c_name", "customer"."c_address", "t0"."o_orderkey" AS "O_ORDERKEY", "t0"."o_orderdate" AS "O_ORDERDATE", ANY_VALUE(CONCAT('Part Name: ', "t"."p_name", ', Brand: ', "t"."p_brand", ', Type: ', "t"."p_type", ', Container: ', "t"."p_container", ', Comment: ', "t"."p_comment")) AS "PART_DETAILS", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE(CONCAT("customer"."c_name", ' (', "customer"."c_address", ')')) AS "CUSTOMER_INFO", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey") ON "lineitem"."l_orderkey" = "t0"."o_orderkey") ON "t"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "t"."p_name", "t"."p_brand", "t"."p_type", "t"."p_container", "t"."p_comment", "supplier"."s_name", "customer"."c_name", "customer"."c_address", "t0"."o_orderkey", "t0"."o_orderdate"