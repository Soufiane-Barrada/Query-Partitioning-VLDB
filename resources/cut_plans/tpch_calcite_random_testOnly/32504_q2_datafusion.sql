SELECT COALESCE("t10"."R_NAME", "t10"."R_NAME") AS "R_NAME", "t10"."REVENUE", "t10"."ORDER_COUNT", "t10"."AVG_SUPPLIER_COST"
FROM (SELECT "region"."r_name" AS "R_NAME", SUM("t5"."l_extendedprice" * (1 - "t5"."l_discount")) AS "REVENUE", COUNT(DISTINCT "t7"."O_ORDERKEY") AS "ORDER_COUNT", ANY_VALUE("t4"."EXPR$0") AS "AVG_SUPPLIER_COST"
FROM (SELECT AVG("TOTAL_SUPPLY_COST") AS "EXPR$0"
FROM (SELECT "ps_partkey" AS "PS_PARTKEY", SUM("ps_supplycost" * "ps_availqty") AS "TOTAL_SUPPLY_COST"
FROM "TPCH"."partsupp"
GROUP BY "ps_partkey") AS "t3") AS "t4"
CROSS JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "part"."p_partkey" = "partsupp0"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp0"."ps_suppkey" INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t5" INNER JOIN (SELECT "t6"."o_orderkey" AS "O_ORDERKEY", "t6"."o_orderdate" AS "O_ORDERDATE", "t6"."o_totalprice" AS "O_TOTALPRICE", RANK() OVER (PARTITION BY "t6"."o_orderkey" ORDER BY "lineitem0"."l_orderkey") AS "RANK_VAL"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t6"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "t6"."o_orderkey" = "lineitem0"."l_orderkey") AS "t7" ON "t5"."l_orderkey" = "t7"."O_ORDERKEY") ON "s1"."C_CUSTKEY" = "t7"."O_ORDERKEY") ON "part"."p_partkey" = "t5"."l_partkey")
GROUP BY "region"."r_name"
ORDER BY 2 DESC NULLS FIRST) AS "t10"