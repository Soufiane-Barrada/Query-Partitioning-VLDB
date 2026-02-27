SELECT COALESCE(ANY_VALUE(CONCAT('Supplier Name: ', "supplier"."s_name", ' | Nation: ', "nation"."n_name", ' | Product: ', "part"."p_name", ' | Comment: ', "partsupp"."ps_comment")), ANY_VALUE(CONCAT('Supplier Name: ', "supplier"."s_name", ' | Nation: ', "nation"."n_name", ' | Product: ', "part"."p_name", ' | Comment: ', "partsupp"."ps_comment"))) AS "DETAILED_INFO", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
LEFT JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
LEFT JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "supplier"."s_comment" LIKE '%reliable%' AND "part"."p_size" >= 10 AND "part"."p_size" <= 50
GROUP BY "supplier"."s_name", "nation"."n_name", "part"."p_name", "partsupp"."ps_comment"