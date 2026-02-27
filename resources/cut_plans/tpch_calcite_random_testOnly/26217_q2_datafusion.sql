SELECT COALESCE("t6"."PART_SUPPLIER", "t6"."PART_SUPPLIER") AS "PART_SUPPLIER", "t6"."UNIQUE_CUSTOMERS", "t6"."TOTAL_REVENUE", "t6"."REGION_NAME", "t6"."COMMENT_EXCERPT"
FROM (SELECT "t1"."p_name", "supplier"."s_name", "s1"."r_name", "t1"."p_comment", ANY_VALUE(CONCAT("t1"."p_name", ' - ', "supplier"."s_name")) AS "PART_SUPPLIER", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", SUM("t2"."l_extendedprice" * (1 - "t2"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("s1"."r_name") AS "REGION_NAME", ANY_VALUE(LEFT("t1"."p_comment", 10)) AS "COMMENT_EXCERPT"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'F') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_container" IN ('BOX', 'PACK')) AS "t1" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t2" ON "t1"."p_partkey" = "t2"."l_partkey" INNER JOIN ("s1" INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "t0"."o_orderkey" = "t2"."l_orderkey"
GROUP BY "t1"."p_name", "supplier"."s_name", "s1"."r_name", "t1"."p_comment"
HAVING SUM("t2"."l_extendedprice" * (1 - "t2"."l_discount")) > 100000.0000
ORDER BY 7 DESC NULLS FIRST, 6) AS "t6"