SELECT COALESCE("t6"."P_NAME", "t6"."P_NAME") AS "P_NAME", CONCAT('Supplier: ', "t6"."s_name", ', Part Type: ', "t6"."p_type", ', Average Supply Cost: ', CAST("t6"."$f4" AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "SUPPLIER_PART_INFO", "t6"."TOTAL_ORDERS", "t6"."TOTAL_REVENUE", "t6"."SHORT_COMMENT"
FROM (SELECT "s1"."p_name" AS "P_NAME", "t2"."s_name", "s1"."p_type", "s1"."p_comment", AVG("s1"."ps_supplycost") AS "$f4", COUNT(DISTINCT "t1"."o_orderkey") AS "TOTAL_ORDERS", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", ANY_VALUE(LEFT("s1"."p_comment", 10)) AS "SHORT_COMMENT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t1"
INNER JOIN "TPCH"."lineitem" ON "t1"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ((SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t2" INNER JOIN "s1" ON "t2"."s_suppkey" = "s1"."ps_suppkey") ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."p_name", "t2"."s_name", "s1"."p_type", "s1"."p_comment"
HAVING COUNT(DISTINCT "t1"."o_orderkey") > 5
ORDER BY 7 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"