SELECT COALESCE("t4"."UNIQUE_CUSTOMERS", "t4"."UNIQUE_CUSTOMERS") AS "UNIQUE_CUSTOMERS", "t4"."TOTAL_REVENUE", "t4"."AVERAGE_PART_PRICE", "t4"."SHORT_PART_NAME", "t4"."SUPPLIER_PREFIX", "t4"."REGION_NAME", "t4"."NATION_NAME", "t4"."COMMENT_LENGTH"
FROM (SELECT "s1"."r_name", "s1"."n_name", SUBSTRING("part"."p_name", 1, 20) AS "$f2", LEFT("supplier"."s_name", 10) AS "$f3", LENGTH("part"."p_comment") AS "$f4", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", AVG("part"."p_retailprice") AS "AVERAGE_PART_PRICE", ANY_VALUE(SUBSTRING("part"."p_name", 1, 20)) AS "SHORT_PART_NAME", ANY_VALUE(LEFT("supplier"."s_name", 10)) AS "SUPPLIER_PREFIX", ANY_VALUE("s1"."r_name") AS "REGION_NAME", ANY_VALUE("s1"."n_name") AS "NATION_NAME", ANY_VALUE(LENGTH("part"."p_comment")) AS "COMMENT_LENGTH"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" <= DATE '1995-12-31') AS "t1" ON "orders"."o_orderkey" = "t1"."l_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "partsupp"."ps_partkey" = "t1"."l_partkey"
GROUP BY "s1"."r_name", "s1"."n_name", SUBSTRING("part"."p_name", 1, 20), LEFT("supplier"."s_name", 10), LENGTH("part"."p_comment")
ORDER BY 7 DESC NULLS FIRST) AS "t4"