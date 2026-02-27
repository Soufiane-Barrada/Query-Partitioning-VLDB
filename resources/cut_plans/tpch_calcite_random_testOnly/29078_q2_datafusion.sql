SELECT COALESCE("t6"."P_PARTKEY", "t6"."P_PARTKEY") AS "P_PARTKEY", "t6"."P_NAME", "t6"."P_RETAILPRICE", "t6"."COMMENT_LENGTH", "t6"."TOTAL_AVAILABLE_QTY", "t6"."SUPPLIER_COUNT", "t6"."C_CUSTKEY", "t6"."C_NAME", "t6"."TOTAL_ORDER_VALUE", "t6"."ORDER_COUNT", "t6"."COMBINED_INFO"
FROM (SELECT "s1"."P_PARTKEY", "s1"."P_NAME", "s1"."P_RETAILPRICE", "s1"."COMMENT_LENGTH", "s1"."TOTAL_AVAILABLE_QTY", "s1"."SUPPLIER_COUNT", "t4"."c_custkey" AS "C_CUSTKEY", "t4"."c_name" AS "C_NAME", "t4"."TOTAL_ORDER_VALUE", "t4"."ORDER_COUNT", CONCAT('Part: ', "s1"."P_NAME", ' | Customer: ', "t4"."c_name") AS "COMBINED_INFO"
FROM "s1"
INNER JOIN (SELECT "customer"."c_custkey", "customer"."c_name", SUM("orders"."o_totalprice") AS "TOTAL_ORDER_VALUE", COUNT(*) AS "ORDER_COUNT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t4" ON LENGTH(CONCAT('Part: ', "s1"."P_NAME", ' | Customer: ', "t4"."c_name")) > 50
FETCH NEXT 100 ROWS ONLY) AS "t6"