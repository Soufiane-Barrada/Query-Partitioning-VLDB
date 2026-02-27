SELECT COALESCE("t"."p_name", "t"."p_name") AS "P_NAME", "supplier"."s_name" AS "S_NAME", "region"."r_name", "nation"."n_name", "supplier"."s_comment", SUM("t0"."l_quantity") AS "TOTAL_QUANTITY", AVG("t0"."l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT", ANY_VALUE(SUBSTRING("supplier"."s_comment", 1, 20)) AS "TRUNCATED_SUPPLIER_COMMENT", ANY_VALUE(CONCAT("region"."r_name", ' - ', "nation"."n_name")) AS "REGION_NATION"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_brand" LIKE 'Brand%1') AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t"."p_partkey" = "t0"."l_partkey"
GROUP BY "t"."p_name", "supplier"."s_name", "region"."r_name", "nation"."n_name", "supplier"."s_comment"