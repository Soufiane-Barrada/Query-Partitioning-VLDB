SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", SUM(CASE WHEN "lineitem"."l_returnflag" = 'R' THEN "lineitem"."l_quantity" ELSE 0.00 END) AS "RETURNED_QUANTITY", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", MAX("lineitem"."l_extendedprice") AS "MAX_PRICE", LISTAGG(DISTINCT CONCAT("supplier"."s_name", ' (', "supplier"."s_phone", ')'), '; ') AS "SUPPLIER_DETAILS"
FROM "TPCH"."part"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
WHERE "part"."p_name" LIKE '%Steel%' AND "orders"."o_orderdate" >= '1997-01-01' AND "orders"."o_orderdate" <= '1997-12-31'
GROUP BY "part"."p_name"
HAVING SUM(CASE WHEN "lineitem"."l_returnflag" = 'R' THEN "lineitem"."l_quantity" ELSE 0.00 END) > 0.00