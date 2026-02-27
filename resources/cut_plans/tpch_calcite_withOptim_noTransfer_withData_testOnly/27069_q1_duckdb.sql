SELECT COALESCE(CONCAT("supplier"."s_name", ' from ', "region"."r_name"), CONCAT("supplier"."s_name", ' from ', "region"."r_name")) AS "FD_COL_0", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "region"."r_name")) AS "SUPPLIER_REGION", COUNT(DISTINCT "t"."o_orderkey") AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", AVG("lineitem"."l_quantity") AS "AVG_QUANTITY_PER_ORDER", LISTAGG(DISTINCT "t0"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_comment" LIKE '%fragile%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t0"."p_partkey"
GROUP BY CONCAT("supplier"."s_name", ' from ', "region"."r_name")