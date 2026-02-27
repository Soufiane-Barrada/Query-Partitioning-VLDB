SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."S_NAME", "t5"."TOTAL_QUANTITY", "t5"."AVG_PRICE", "t5"."ORDER_COUNT", "t5"."TRUNCATED_SUPPLIER_COMMENT", "t5"."REGION_NATION"
FROM (SELECT "t0"."p_name" AS "P_NAME", "s1"."s_name" AS "S_NAME", "s1"."r_name", "s1"."n_name", "s1"."s_comment", SUM("t1"."l_quantity") AS "TOTAL_QUANTITY", AVG("t1"."l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT", ANY_VALUE(SUBSTRING("s1"."s_comment", 1, 20)) AS "TRUNCATED_SUPPLIER_COMMENT", ANY_VALUE(CONCAT("s1"."r_name", ' - ', "s1"."n_name")) AS "REGION_NATION"
FROM "s1"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_brand" LIKE 'Brand%1') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" INNER JOIN "TPCH"."orders" ON "t1"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t0"."p_partkey" = "t1"."l_partkey"
GROUP BY "t0"."p_name", "s1"."s_name", "s1"."r_name", "s1"."n_name", "s1"."s_comment"
HAVING SUM("t1"."l_quantity") > 1000.00
ORDER BY 6 DESC NULLS FIRST, 7) AS "t5"