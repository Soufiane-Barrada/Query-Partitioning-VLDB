SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."SHORT_COMMENT", "t5"."SUPPLIER_REGION_INFO", "t5"."ORDER_COUNT", "t5"."TOTAL_QUANTITY", "t5"."AVG_PRICE", CASE WHEN "t5"."$f10" > 0.00 THEN 'Discounted   ' ELSE 'Regular Price' END AS "PRICE_TYPE"
FROM (SELECT "t1"."p_partkey", "t1"."p_name" AS "P_NAME", "t1"."p_comment", "supplier"."s_name", "s1"."r_name", ANY_VALUE(SUBSTRING("t1"."p_comment", 1, 10)) AS "SHORT_COMMENT", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ', Region: ', "s1"."r_name")) AS "SUPPLIER_REGION_INFO", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", AVG("lineitem"."l_extendedprice") AS "AVG_PRICE", SUM("lineitem"."l_discount") AS "$f10"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0"
INNER JOIN "TPCH"."lineitem" ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"
GROUP BY "t1"."p_partkey", "t1"."p_name", "t1"."p_comment", "supplier"."s_name", "s1"."r_name"
HAVING COUNT(DISTINCT "t0"."o_orderkey") > 5
ORDER BY 9 DESC NULLS FIRST, 10) AS "t5"