SELECT COALESCE(CONCAT("supplier"."s_name", ' (', "region"."r_name", ')'), CONCAT("supplier"."s_name", ' (', "region"."r_name", ')')) AS "FD_COL_0", ANY_VALUE(CONCAT("supplier"."s_name", ' (', "region"."r_name", ')')) AS "SUPPLIER_REGION", SUM("t0"."l_quantity") AS "TOTAL_QUANTITY", AVG("t0"."l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "t"."o_orderkey") AS "ORDER_COUNT", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" ON "t"."o_orderkey" = "t0"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t0"."l_partkey" = "part"."p_partkey"
GROUP BY CONCAT("supplier"."s_name", ' (', "region"."r_name", ')')