SELECT COALESCE("t10"."REGION", "t10"."REGION") AS "REGION", "t10"."CUSTOMER_COUNT", "t10"."TOTAL_SUPPLIER_BALANCE", "t10"."AVERAGE_ORDER_VALUE"
FROM (SELECT "t3"."r_name", ANY_VALUE("t3"."r_name") AS "REGION", COUNT(DISTINCT "t3"."c_custkey") AS "CUSTOMER_COUNT", SUM("t3"."s_acctbal") AS "TOTAL_SUPPLIER_BALANCE", AVG("t8"."O_TOTALPRICE") AS "AVERAGE_ORDER_VALUE"
FROM (SELECT "s1"."r_regionkey", "s1"."r_name", "s1"."r_comment", "s1"."n_nationkey", "s1"."n_name", "s1"."n_regionkey", "s1"."n_comment", "s1"."s_suppkey", "s1"."s_name", "s1"."s_address", "s1"."s_nationkey", "s1"."s_phone", "s1"."s_acctbal", "s1"."s_comment", CAST("customer"."c_custkey" AS BIGINT) AS "c_custkey", CAST("customer"."c_name" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "c_name", CAST("customer"."c_address" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "c_address", CAST("customer"."c_nationkey" AS INTEGER) AS "c_nationkey", CAST("customer"."c_phone" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "c_phone", CAST("customer"."c_acctbal" AS DECIMAL(15, 2)) AS "c_acctbal", CAST("customer"."c_mktsegment" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "c_mktsegment", CAST("customer"."c_comment" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "c_comment"
FROM "TPCH"."customer"
INNER JOIN "s1" ON "customer"."c_nationkey" = "s1"."n_nationkey") AS "t3"
LEFT JOIN (SELECT "t7"."o_orderkey" AS "O_ORDERKEY", "t7"."o_totalprice" AS "O_TOTALPRICE", "t7"."o_orderstatus" AS "O_ORDERSTATUS", "t6"."TOTAL_REVENUE", "t6"."PART_COUNT", ROW_NUMBER() OVER (PARTITION BY "t7"."o_orderstatus" ORDER BY "t6"."TOTAL_REVENUE" DESC NULLS FIRST) AS "RN"
FROM (SELECT "t4"."o_orderkey" AS "O_ORDERKEY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "lineitem"."l_partkey") AS "PART_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" <= DATE '1995-12-31') AS "t4"
INNER JOIN "TPCH"."lineitem" ON "t4"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "t4"."o_orderkey") AS "t6"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_totalprice" > 10000.00) AS "t7" ON "t6"."O_ORDERKEY" = "t7"."o_orderkey") AS "t8" ON "t3"."c_custkey" = "t8"."O_ORDERKEY"
GROUP BY "t3"."r_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t10"