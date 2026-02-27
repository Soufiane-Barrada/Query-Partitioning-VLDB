SELECT COALESCE("t4"."SUPPLIER_REGION", "t4"."SUPPLIER_REGION") AS "SUPPLIER_REGION", "t4"."ORDER_COUNT", "t4"."TOTAL_REVENUE", "t4"."AVG_QUANTITY_PER_ORDER", "t4"."PART_NAMES"
FROM (SELECT CONCAT("supplier"."s_name", ' from ', "region"."r_name") AS "$f0", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "region"."r_name")) AS "SUPPLIER_REGION", COUNT(DISTINCT "t1"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", AVG("lineitem"."l_quantity") AS "AVG_QUANTITY_PER_ORDER", LISTAGG(DISTINCT "s1"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t1"
INNER JOIN "TPCH"."lineitem" ON "t1"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY CONCAT("supplier"."s_name", ' from ', "region"."r_name")
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"