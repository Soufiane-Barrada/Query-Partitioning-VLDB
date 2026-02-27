SELECT COALESCE("t3"."P_PARTKEY", "t3"."P_PARTKEY") AS "P_PARTKEY", "t3"."P_NAME", "t3"."SUPPLIER_NAME", "t3"."PS_SUPPLYCOST", "t3"."PS_AVAILQTY", "t3"."TOTAL_REVENUE"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "supplier"."s_name", "partsupp"."ps_supplycost" AS "PS_SUPPLYCOST", "partsupp"."ps_availqty" AS "PS_AVAILQTY", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN "TPCH"."lineitem" ON "s1"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."supplier" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "part"."p_partkey"
GROUP BY "part"."p_partkey", "part"."p_name", "supplier"."s_name", "partsupp"."ps_supplycost", "partsupp"."ps_availqty"
ORDER BY 7 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"