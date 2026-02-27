SELECT COALESCE("t0"."n_name", "t0"."n_name") AS "N_NAME", "t0"."TOTAL_ACCTBAL", "t5"."P_PARTKEY", "t5"."TOTAL_SUPPLYCOST", "t3"."C_CUSTKEY", "t3"."TOTAL_ORDERVALUE"
FROM (SELECT "t"."n_name", SUM("supplier"."s_acctbal") AS "TOTAL_ACCTBAL"
FROM (SELECT *
FROM "TPCH"."nation"
WHERE "n_name" = 'USA') AS "t"
INNER JOIN "TPCH"."supplier" ON "t"."n_nationkey" = "supplier"."s_nationkey"
GROUP BY "t"."n_name") AS "t0",
((SELECT "customer"."c_custkey" AS "C_CUSTKEY", SUM("orders"."o_totalprice") AS "TOTAL_ORDERVALUE", SUM("orders"."o_totalprice") > 100000.00 AS "$f2"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey"
HAVING SUM("orders"."o_totalprice") > 100000.00) AS "t3", (SELECT "part"."p_partkey" AS "P_PARTKEY", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "part"."p_partkey") AS "t5")