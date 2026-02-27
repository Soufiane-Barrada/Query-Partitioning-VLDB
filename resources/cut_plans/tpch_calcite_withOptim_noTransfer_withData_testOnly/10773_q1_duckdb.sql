SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "p_partkey", "part"."p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", AVG("lineitem"."l_discount") AS "AVERAGE_DISCOUNT", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS"
FROM "TPCH"."part"
INNER JOIN ("TPCH"."orders" INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "part"."p_partkey", "part"."p_name"