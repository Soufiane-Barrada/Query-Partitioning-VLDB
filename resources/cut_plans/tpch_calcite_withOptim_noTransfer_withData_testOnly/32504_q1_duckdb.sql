SELECT COALESCE("region"."r_name", "region"."r_name") AS "R_NAME", "t4"."l_extendedprice" * (1 - "t4"."l_discount") AS "FD_COL_1", "t6"."O_ORDERKEY", "t1"."EXPR$0" AS "FD_COL_3"
FROM (SELECT AVG("TOTAL_SUPPLY_COST") AS "EXPR$0"
FROM (SELECT "ps_partkey" AS "PS_PARTKEY", SUM("ps_supplycost" * "ps_availqty") AS "TOTAL_SUPPLY_COST"
FROM "TPCH"."partsupp"
GROUP BY "ps_partkey") AS "t0") AS "t1"
CROSS JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "part"."p_partkey" = "partsupp0"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp0"."ps_suppkey" INNER JOIN ((SELECT "c_custkey" AS "C_CUSTKEY", "c_name" AS "C_NAME", "c_acctbal" AS "C_ACCTBAL", CASE WHEN "c_phone" <> '' THEN CAST(CASE WHEN "c_phone" = '' THEN NULL ELSE "c_phone" END AS VARCHAR CHARACTER SET "ISO-8859-1") ELSE 'N/A' END AS "CONTACT_NUMBER"
FROM "TPCH"."customer"
WHERE "c_acctbal" > 1000.00) AS "t3" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t4" INNER JOIN (SELECT "t5"."o_orderkey" AS "O_ORDERKEY", "t5"."o_orderdate" AS "O_ORDERDATE", "t5"."o_totalprice" AS "O_TOTALPRICE", RANK() OVER (PARTITION BY "t5"."o_orderkey" ORDER BY "lineitem0"."l_orderkey") AS "RANK_VAL"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t5"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "t5"."o_orderkey" = "lineitem0"."l_orderkey") AS "t6" ON "t4"."l_orderkey" = "t6"."O_ORDERKEY") ON "t3"."C_CUSTKEY" = "t6"."O_ORDERKEY") ON "part"."p_partkey" = "t4"."l_partkey")