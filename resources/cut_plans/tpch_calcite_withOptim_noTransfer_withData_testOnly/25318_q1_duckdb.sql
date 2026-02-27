SELECT COALESCE(CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ' region, ', "t"."r_name"), CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ' region, ', "t"."r_name")) AS "FD_COL_0", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ' region, ', "t"."r_name")) AS "SUPPLIER_REGION_NAME", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDERS_COUNT", AVG(CASE WHEN "customer"."c_mktsegment" = 'BUILDING' THEN CAST("t0"."l_extendedprice" AS DECIMAL(15, 2)) ELSE NULL END) AS "AVG_PRICE_BUILDING", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE '%East%') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1996-12-31') AS "t0" ON "orders"."o_orderkey" = "t0"."l_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "part"."p_partkey" = "t0"."l_partkey"
GROUP BY CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ' region, ', "t"."r_name")