SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", CONCAT('Supplier: ', "supplier"."s_name", ', Part Type: ', "part"."p_type", ', Average Supply Cost: ', CAST(AVG("partsupp"."ps_supplycost") AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "SUPPLIER_PART_INFO", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", ANY_VALUE(LEFT("part"."p_comment", 10)) AS "SHORT_COMMENT"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "part"."p_size" > 20 AND "supplier"."s_acctbal" > 1000.00 AND "orders"."o_orderstatus" = 'O'
GROUP BY "part"."p_name", "supplier"."s_name", "part"."p_type", "part"."p_comment"
HAVING COUNT(DISTINCT "orders"."o_orderkey") > 5