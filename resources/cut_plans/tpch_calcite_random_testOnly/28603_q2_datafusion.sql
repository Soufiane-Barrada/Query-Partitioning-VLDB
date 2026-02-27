SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."S_NAME", "t4"."C_NAME", "t4"."N_NAME", "t4"."R_NAME", "t4"."TOTAL_ORDERS", "t4"."TOTAL_REVENUE", "t4"."AVG_RETAIL_PRICE", "t4"."COMMENTS_AGGREGATED"
FROM (SELECT "t0"."p_name" AS "P_NAME", "s1"."s_name" AS "S_NAME", "customer"."c_name" AS "C_NAME", "s1"."n_name" AS "N_NAME", "s1"."r_name" AS "R_NAME", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", AVG("t0"."p_retailprice") AS "AVG_RETAIL_PRICE", LISTAGG(DISTINCT "t1"."l_comment", '; ') AS "COMMENTS_AGGREGATED"
FROM "s1"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_comment" LIKE '%special%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" ON "orders"."o_orderkey" = "t1"."l_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t0"."p_partkey" = "t1"."l_partkey"
GROUP BY "t0"."p_name", "s1"."s_name", "customer"."c_name", "s1"."n_name", "s1"."r_name"
ORDER BY 7 DESC NULLS FIRST, 6 DESC NULLS FIRST) AS "t4"