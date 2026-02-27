SELECT COALESCE("t6"."PART_NAME", "t6"."PART_NAME") AS "PART_NAME", "t6"."SUPPLIER_NAME", "t6"."NATION_NAME", "t6"."TOTAL_QUANTITY_SOLD", "t6"."AVG_PRICE_AFTER_DISCOUNT", "t6"."TOTAL_ORDERS", "t6"."ORDER_PRIORITIES", "t6"."LATEST_SHIP_DATE"
FROM (SELECT "s1"."p_name", "supplier"."s_name", "nation"."n_name", ANY_VALUE("s1"."p_name") AS "PART_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY_SOLD", AVG("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "AVG_PRICE_AFTER_DISCOUNT", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT "orders"."o_orderpriority", ', ') AS "ORDER_PRIORITIES", MAX("lineitem"."l_shipdate") AS "LATEST_SHIP_DATE"
FROM "TPCH"."orders"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ((SELECT "r_name"
FROM "TPCH"."region"
WHERE "r_comment" LIKE '%east%'
GROUP BY "r_name") AS "t2" INNER JOIN "TPCH"."nation" ON "t2"."r_name" = "nation"."n_name" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey") ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."p_name", "supplier"."s_name", "nation"."n_name"
HAVING SUM("lineitem"."l_quantity") > 100.00
ORDER BY 7 DESC NULLS FIRST) AS "t6"