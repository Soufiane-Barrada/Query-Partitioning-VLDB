SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."S_NAME", "t5"."N_NAME", "t5"."TOTAL_ORDERS", "t5"."AVG_ORDER_VALUE"
FROM (SELECT "part"."p_name" AS "P_NAME", "s1"."s_name" AS "S_NAME", "s1"."n_name" AS "N_NAME", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", AVG("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "AVG_ORDER_VALUE"
FROM "TPCH"."orders"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" <= DATE '1996-12-31') AS "t1" ON "orders"."o_orderkey" = "t1"."l_orderkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."part" INNER JOIN (SELECT *
FROM "TPCH"."partsupp"
WHERE "ps_availqty" > 10) AS "t2" ON "part"."p_partkey" = "t2"."ps_partkey") ON "s1"."s_suppkey" = "t2"."ps_suppkey") ON "t1"."l_partkey" = "part"."p_partkey"
GROUP BY "part"."p_name", "s1"."s_name", "s1"."n_name"
ORDER BY 4 DESC NULLS FIRST, 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5"