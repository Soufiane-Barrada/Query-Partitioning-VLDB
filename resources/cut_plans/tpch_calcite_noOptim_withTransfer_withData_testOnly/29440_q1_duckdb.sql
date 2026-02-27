SELECT COALESCE(ANY_VALUE(SUBSTRING("part"."p_name", 1, 10)), ANY_VALUE(SUBSTRING("part"."p_name", 1, 10))) AS "SHORT_NAME", COUNT(DISTINCT "supplier"."s_nationkey") AS "NATION_COUNT", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST", LISTAGG(DISTINCT "customer"."c_mktsegment", ', ') AS "MARKET_SEGMENTS", MAX("orders"."o_totalprice") AS "MAX_ORDER_PRICE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."customer" ON "supplier"."s_nationkey" = "customer"."c_nationkey"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
WHERE "part"."p_name" LIKE 'Z%' AND "orders"."o_orderdate" >= '1996-01-01' AND "orders"."o_orderdate" <= '1996-12-31'
GROUP BY SUBSTRING("part"."p_name", 1, 10)
HAVING COUNT(DISTINCT "supplier"."s_nationkey") > 1