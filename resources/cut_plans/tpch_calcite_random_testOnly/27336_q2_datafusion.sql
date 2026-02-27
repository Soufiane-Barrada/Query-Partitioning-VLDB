SELECT COALESCE("t4"."SUPPLIER_CUSTOMER", "t4"."SUPPLIER_CUSTOMER") AS "SUPPLIER_CUSTOMER", "t4"."TRUNCATED_PART_NAME", "t4"."P_BRAND", "t4"."SANITIZED_SUPPLIER_COMMENT", "t4"."TOTAL_REVENUE", CASE WHEN "t4"."TOTAL_REVENUE" > 10000.0000 THEN 'High Revenue  ' WHEN "t4"."TOTAL_REVENUE" >= 5000.0000 AND "t4"."TOTAL_REVENUE" <= 10000.0000 THEN 'Medium Revenue' ELSE 'Low Revenue   ' END AS "REVENUE_CATEGORY", "t4"."ORDER_COUNT"
FROM (SELECT "customer"."c_name", "s1"."s_name", "s1"."p_name", "s1"."p_brand" AS "P_BRAND", "s1"."s_comment", ANY_VALUE(CONCAT("customer"."c_name", ' from ', "s1"."s_name")) AS "SUPPLIER_CUSTOMER", ANY_VALUE(SUBSTRING("s1"."p_name", 1, 20)) AS "TRUNCATED_PART_NAME", ANY_VALUE(REGEXP_REPLACE("s1"."s_comment", '[^a-zA-Z0-9 ]', '')) AS "SANITIZED_SUPPLIER_COMMENT", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT"
FROM "TPCH"."customer"
INNER JOIN ("s1" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01') AS "t1" ON "orders"."o_orderkey" = "t1"."l_orderkey") ON "s1"."ps_partkey" = "t1"."l_partkey") ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_name", "s1"."s_name", "s1"."p_name", "s1"."p_brand", "s1"."s_comment"
ORDER BY 9 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"