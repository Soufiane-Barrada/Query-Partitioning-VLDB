SELECT COALESCE(CONCAT('Supplier: ', "supplier"."s_name"), CONCAT('Supplier: ', "supplier"."s_name")) AS "FD_COL_0", LEFT("part"."p_name", 20) AS "FD_COL_1", REPLACE("part"."p_comment", 'obsolete', 'legacy') AS "FD_COL_2", "region"."r_name", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name")) AS "SUPPLIER_INFO", ANY_VALUE(LEFT("part"."p_name", 20)) AS "PART_NAME", ANY_VALUE(REPLACE("part"."p_comment", 'obsolete', 'legacy')) AS "UPDATED_COMMENT", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" ON "t"."o_orderkey" = "t0"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t0"."l_partkey" = "part"."p_partkey"
GROUP BY CONCAT('Supplier: ', "supplier"."s_name"), LEFT("part"."p_name", 20), REPLACE("part"."p_comment", 'obsolete', 'legacy'), "region"."r_name"