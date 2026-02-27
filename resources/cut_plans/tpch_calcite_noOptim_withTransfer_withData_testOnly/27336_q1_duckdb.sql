SELECT COALESCE(ANY_VALUE(CONCAT("customer"."c_name", ' from ', "supplier"."s_name")), ANY_VALUE(CONCAT("customer"."c_name", ' from ', "supplier"."s_name"))) AS "SUPPLIER_CUSTOMER", ANY_VALUE(SUBSTRING("part"."p_name", 1, 20)) AS "TRUNCATED_PART_NAME", "part"."p_brand" AS "P_BRAND", ANY_VALUE(REGEXP_REPLACE("supplier"."s_comment", '[^a-zA-Z0-9 ]', '')) AS "SANITIZED_SUPPLIER_COMMENT", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", CASE WHEN SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 10000.0000 THEN 'High Revenue  ' WHEN SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) >= 5000.0000 AND SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) <= 10000.0000 THEN 'Medium Revenue' ELSE 'Low Revenue   ' END AS "REVENUE_CATEGORY", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT"
FROM "TPCH"."lineitem"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
INNER JOIN "TPCH"."partsupp" ON "lineitem"."l_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
WHERE "part"."p_retailprice" >= 10.00 AND "part"."p_retailprice" <= 100.00 AND "lineitem"."l_shipdate" >= '1997-01-01'
GROUP BY "customer"."c_name", "supplier"."s_name", "part"."p_name", "part"."p_brand", "supplier"."s_comment"