SELECT COALESCE("t4"."SUPPLIER_REGION_NAME", "t4"."SUPPLIER_REGION_NAME") AS "SUPPLIER_REGION_NAME", "t4"."TOTAL_REVENUE", "t4"."ORDERS_COUNT", "t4"."AVG_PRICE_BUILDING", "t4"."PART_NAMES"
FROM (SELECT CONCAT("supplier"."s_name", ' from ', "s1"."n_name", ' region, ', "s1"."r_name") AS "$f0", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "s1"."n_name", ' region, ', "s1"."r_name")) AS "SUPPLIER_REGION_NAME", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDERS_COUNT", AVG(CASE WHEN "customer"."c_mktsegment" = 'BUILDING' THEN CAST("t1"."l_extendedprice" AS DECIMAL(15, 2)) ELSE NULL END) AS "AVG_PRICE_BUILDING", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PART_NAMES"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1996-12-31') AS "t1" ON "orders"."o_orderkey" = "t1"."l_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "part"."p_partkey" = "t1"."l_partkey"
GROUP BY CONCAT("supplier"."s_name", ' from ', "s1"."n_name", ' region, ', "s1"."r_name")
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"