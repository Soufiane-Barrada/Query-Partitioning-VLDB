SELECT COALESCE("t7"."N_NAME", "t7"."N_NAME") AS "N_NAME", "t7"."TOTAL_ACCTBAL", "t7"."TOTAL_SUPPLYCOST", "t7"."TOTAL_ORDERVALUE", "t10"."TOTAL_LINES"
FROM (SELECT "t4"."N_NAME", "t4"."TOTAL_ACCTBAL", "t6"."P_PARTKEY", "t6"."TOTAL_SUPPLYCOST", "s1"."C_CUSTKEY", "s1"."TOTAL_ORDERVALUE"
FROM (SELECT "nation"."n_name" AS "N_NAME", SUM("supplier"."s_acctbal") AS "TOTAL_ACCTBAL"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
GROUP BY "nation"."n_name") AS "t4"
INNER JOIN (SELECT "part"."p_partkey" AS "P_PARTKEY", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "part"."p_partkey") AS "t6" ON "t4"."N_NAME" = 'USA'
INNER JOIN "s1" ON "s1"."FD_COL_2") AS "t7"
INNER JOIN (SELECT "orders0"."o_orderkey" AS "O_ORDERKEY", COUNT(*) AS "TOTAL_LINES", COUNT(*) > 5 AS "$f2"
FROM "TPCH"."orders" AS "orders0"
INNER JOIN "TPCH"."lineitem" ON "orders0"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "orders0"."o_orderkey") AS "t10" ON "t10"."$f2"
ORDER BY "t7"."TOTAL_ACCTBAL" DESC NULLS FIRST, "t7"."TOTAL_SUPPLYCOST" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY