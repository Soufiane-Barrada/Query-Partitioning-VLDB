SELECT COALESCE(ANY_VALUE("s1"."C_NAME"), ANY_VALUE("s1"."C_NAME")) AS "TOP_CUSTOMER", ANY_VALUE("t11"."S_NAME") AS "TOP_SUPPLIER", ROUND(SUM("t14"."REVENUE"), 2) AS "TOTAL_REVENUE", ROUND(SUM("t11"."TOTAL_SUPPLYCOST"), 2) AS "TOTAL_SUPPLY_COST", ANY_VALUE("s1"."TOTAL_REVENUE") AS "CUSTOMER_REVENUE"
FROM (SELECT "t9"."S_SUPPKEY", "t9"."S_NAME", "t9"."TOTAL_SUPPLYCOST", "t7"."EXPR$0"
FROM (SELECT MAX("t6"."TOTAL_SUPPLYCOST") AS "EXPR$0"
FROM (SELECT "supplier"."s_suppkey" AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name") AS "t6") AS "t7",
(SELECT "supplier0"."s_suppkey" AS "S_SUPPKEY", "supplier0"."s_name" AS "S_NAME", SUM("partsupp0"."ps_supplycost" * "partsupp0"."ps_availqty") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."supplier" AS "supplier0"
INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "supplier0"."s_suppkey" = "partsupp0"."ps_suppkey"
GROUP BY "supplier0"."s_suppkey", "supplier0"."s_name") AS "t9"
WHERE "t9"."TOTAL_SUPPLYCOST" = "t7"."EXPR$0") AS "t11"
CROSS JOIN ("s1" INNER JOIN (SELECT "t12"."o_orderkey" AS "O_ORDERKEY", "t12"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")) AS "REVENUE", COUNT(DISTINCT "lineitem0"."l_suppkey") AS "SUPPLIER_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1994-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t12"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "t12"."o_orderkey" = "lineitem0"."l_orderkey"
GROUP BY "t12"."o_orderkey", "t12"."o_orderdate") AS "t14" ON "s1"."C_CUSTKEY" = "t14"."O_ORDERKEY")
GROUP BY "t11"."S_NAME", "s1"."C_NAME", "s1"."TOTAL_REVENUE"