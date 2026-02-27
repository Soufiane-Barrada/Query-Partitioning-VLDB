SELECT COALESCE("t2"."P_PARTKEY", "t2"."P_PARTKEY") AS "P_PARTKEY", "t2"."P_NAME", "t2"."P_RETAILPRICE", "t2"."COMMENT_LENGTH", "t2"."TOTAL_AVAILABLE_QTY", "t2"."SUPPLIER_COUNT", "t2"."$f6" AS "FD_COL_6", "t3"."c_custkey" AS "C_CUSTKEY", "t3"."c_name" AS "C_NAME", "t3"."TOTAL_ORDER_VALUE", "t3"."ORDER_COUNT"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "part"."p_retailprice" AS "P_RETAILPRICE", ANY_VALUE(LENGTH("part"."p_comment")) AS "COMMENT_LENGTH", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QTY", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", COUNT(DISTINCT "partsupp"."ps_suppkey") > 0 AS "$f6"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "part"."p_partkey", "part"."p_name", "part"."p_retailprice", LENGTH("part"."p_comment")
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 0) AS "t2"
INNER JOIN (SELECT "customer"."c_custkey", "customer"."c_name", SUM("orders"."o_totalprice") AS "TOTAL_ORDER_VALUE", COUNT(*) AS "ORDER_COUNT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t3" ON LENGTH(CONCAT('Part: ', "t2"."P_NAME", ' | Customer: ', "t3"."c_name")) > 50