SELECT COALESCE(ANY_VALUE("nation"."n_name"), ANY_VALUE("nation"."n_name")) AS "NATION_NAME", SUM(CAST("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS DECIMAL(19, 4))) AS "TOTAL_SALES", COUNT(DISTINCT "orders"."o_orderkey") AS "ORDER_COUNT", MAX("lineitem"."l_shipdate") AS "LAST_SHIP_DATE", LISTAGG(DISTINCT "part"."p_name", ', ') AS "PRODUCT_NAMES"
FROM "TPCH"."lineitem"
LEFT JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
LEFT JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
LEFT JOIN "TPCH"."nation" ON "customer"."c_nationkey" = "nation"."n_nationkey"
LEFT JOIN "TPCH"."part" ON "lineitem"."l_partkey" = "part"."p_partkey"
WHERE "lineitem"."l_shipdate" >= DATE '1997-01-01' AND "lineitem"."l_shipdate" < DATE '1997-12-31' AND "part"."p_size" IS NOT NULL
GROUP BY "nation"."n_name"
HAVING SUM(CAST("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS DECIMAL(19, 4))) > (((SELECT AVG(SUM("lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount")))
FROM "TPCH"."lineitem" AS "lineitem0"
INNER JOIN "TPCH"."orders" AS "orders0" ON "lineitem0"."l_orderkey" = "orders0"."o_orderkey"
GROUP BY "orders0"."o_orderkey")))