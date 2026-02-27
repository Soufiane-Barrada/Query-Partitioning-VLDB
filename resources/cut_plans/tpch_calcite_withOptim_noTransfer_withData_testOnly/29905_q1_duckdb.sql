SELECT COALESCE("t0"."p_partkey", "t0"."p_partkey") AS "p_partkey", "t0"."p_name" AS "P_NAME", "t0"."p_comment", "supplier"."s_name", "region"."r_name", ANY_VALUE(SUBSTRING("t0"."p_comment", 1, 10)) AS "SHORT_COMMENT", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ', Region: ', "region"."r_name")) AS "SUPPLIER_REGION_INFO", COUNT(DISTINCT "t"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", AVG("lineitem"."l_extendedprice") AS "AVG_PRICE", SUM("lineitem"."l_discount") AS "FD_COL_10"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t0"."p_partkey"
GROUP BY "t0"."p_partkey", "t0"."p_name", "t0"."p_comment", "supplier"."s_name", "region"."r_name"
HAVING COUNT(DISTINCT "t"."o_orderkey") > 5