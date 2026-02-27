SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "p_name", "supplier"."s_name", "region"."r_name", "t0"."p_comment", ANY_VALUE(CONCAT("t0"."p_name", ' - ', "supplier"."s_name")) AS "PART_SUPPLIER", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE(LEFT("t0"."p_comment", 10)) AS "COMMENT_EXCERPT"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'F') AS "t" ON "customer"."c_custkey" = "t"."o_custkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_container" IN ('BOX', 'PACK')) AS "t0" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" ON "t0"."p_partkey" = "t1"."l_partkey" INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."o_orderkey" = "t1"."l_orderkey"
GROUP BY "t0"."p_name", "supplier"."s_name", "region"."r_name", "t0"."p_comment"