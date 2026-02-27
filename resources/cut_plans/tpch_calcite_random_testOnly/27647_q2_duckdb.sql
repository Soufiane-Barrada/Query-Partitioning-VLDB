SELECT COALESCE("t5"."SUPPLIER_REGION", "t5"."SUPPLIER_REGION") AS "SUPPLIER_REGION", "t5"."TOTAL_QUANTITY", "t5"."AVG_PRICE", "t5"."ORDER_COUNT", "t5"."PART_NAMES"
FROM (SELECT CONCAT("supplier"."s_name", ' (', "region"."r_name", ')') AS "$f0", ANY_VALUE(CONCAT("supplier"."s_name", ' (', "region"."r_name", ')')) AS "SUPPLIER_REGION", SUM("t1"."l_quantity") AS "TOTAL_QUANTITY", AVG("t1"."l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "t0"."o_orderkey") AS "ORDER_COUNT", LISTAGG(DISTINCT "s1"."p_name", ', ') AS "PART_NAMES"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t0"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" ON "t0"."o_orderkey" = "t1"."l_orderkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey") ON "t1"."l_partkey" = "s1"."p_partkey"
GROUP BY CONCAT("supplier"."s_name", ' (', "region"."r_name", ')')
HAVING SUM("t1"."l_quantity") > 100.00
ORDER BY 3 DESC NULLS FIRST) AS "t5"