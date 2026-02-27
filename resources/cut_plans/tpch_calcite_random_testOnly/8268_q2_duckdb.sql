SELECT COALESCE("t4"."NATION", "t4"."NATION") AS "NATION", "t4"."TOTAL_REVENUE", "t4"."NUM_CUSTOMERS", "t4"."AVG_DISCOUNTED_PRICE"
FROM (SELECT "s1"."n_name", ANY_VALUE("s1"."n_name") AS "NATION", SUM("s1"."o_totalprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "s1"."c_custkey") AS "NUM_CUSTOMERS", AVG("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "AVG_DISCOUNTED_PRICE"
FROM "s1"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "supplier"."s_suppkey") ON "s1"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "s1"."n_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"