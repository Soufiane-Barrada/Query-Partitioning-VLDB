SELECT COALESCE("t1"."p_name", "t1"."p_name") AS "p_name", "supplier"."s_name", "nation"."n_name", ANY_VALUE("t1"."p_name") AS "PART_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY_SOLD", AVG("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "AVG_PRICE_AFTER_DISCOUNT", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", LISTAGG(DISTINCT "orders"."o_orderpriority", ', ') AS "ORDER_PRIORITIES", MAX("lineitem"."l_shipdate") AS "LATEST_SHIP_DATE"
FROM "TPCH"."orders"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ((SELECT "r_name"
FROM "TPCH"."region"
WHERE "r_comment" LIKE '%east%'
GROUP BY "r_name") AS "t0" INNER JOIN "TPCH"."nation" ON "t0"."r_name" = "nation"."n_name" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'rubber%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t1"."p_partkey"
GROUP BY "t1"."p_name", "supplier"."s_name", "nation"."n_name"
HAVING SUM("lineitem"."l_quantity") > 100.00