SELECT COALESCE("part"."p_name", "part"."p_name") AS "p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS"
FROM "TPCH"."part"
INNER JOIN ("TPCH"."orders" INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "part"."p_name"