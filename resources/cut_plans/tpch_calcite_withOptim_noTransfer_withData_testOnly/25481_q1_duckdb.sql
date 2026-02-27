SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "P_NAME", "t0"."s_name", "t1"."p_type", "t1"."p_comment", AVG("partsupp"."ps_supplycost") AS "FD_COL_4", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", ANY_VALUE(LEFT("t1"."p_comment", 10)) AS "SHORT_COMMENT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ((SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t0" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 20) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"
GROUP BY "t1"."p_name", "t0"."s_name", "t1"."p_type", "t1"."p_comment"