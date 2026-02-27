SELECT COALESCE("t6"."P_NAME", "t6"."P_NAME") AS "P_NAME", "t6"."SUPPLIER_COUNT", "t6"."AVG_SUPPLY_COST", "t6"."TOTAL_DISCOUNTED_QUANTITY", "t6"."MAX_RECENT_ORDER_VALUE", "t6"."SUPPLIER_NAMES"
FROM (SELECT "t2"."p_name" AS "P_NAME", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST", SUM(CASE WHEN "lineitem"."l_discount" > 0.00 THEN "lineitem"."l_quantity" ELSE 0.00 END) AS "TOTAL_DISCOUNTED_QUANTITY", MAX(CASE WHEN "orders"."o_orderdate" > DATE '1997-01-01' THEN CAST("orders"."o_totalprice" AS DECIMAL(15, 2)) ELSE NULL END) AS "MAX_RECENT_ORDER_VALUE", LISTAGG(DISTINCT "s1"."s_name", ', ') AS "SUPPLIER_NAMES"
FROM "TPCH"."orders"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%raw%') AS "t2" INNER JOIN "TPCH"."partsupp" ON "t2"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "t2"."p_partkey"
GROUP BY "t2"."p_name"
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 5
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"