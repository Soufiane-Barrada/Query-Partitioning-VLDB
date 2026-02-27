SELECT COALESCE("t4"."SUPPLIER_INFO", "t4"."SUPPLIER_INFO") AS "SUPPLIER_INFO", "t4"."PART_NAME", "t4"."UPDATED_COMMENT", "t4"."TOTAL_ORDERS", "t4"."TOTAL_REVENUE", "t4"."REGION_NAME"
FROM (SELECT CONCAT('Supplier: ', "supplier"."s_name") AS "$f0", LEFT("part"."p_name", 20) AS "$f1", REPLACE("part"."p_comment", 'obsolete', 'legacy') AS "$f2", "s1"."r_name", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name")) AS "SUPPLIER_INFO", ANY_VALUE(LEFT("part"."p_name", 20)) AS "PART_NAME", ANY_VALUE(REPLACE("part"."p_comment", 'obsolete', 'legacy')) AS "UPDATED_COMMENT", COUNT(DISTINCT "t0"."o_orderkey") AS "TOTAL_ORDERS", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("s1"."r_name") AS "REGION_NAME"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t0"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" ON "t0"."o_orderkey" = "t1"."l_orderkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."l_partkey" = "part"."p_partkey"
GROUP BY CONCAT('Supplier: ', "supplier"."s_name"), LEFT("part"."p_name", 20), REPLACE("part"."p_comment", 'obsolete', 'legacy'), "s1"."r_name"
ORDER BY 9 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"