SELECT COALESCE(SUBSTRING("t"."p_name", 1, 10), SUBSTRING("t"."p_name", 1, 10)) AS "FD_COL_0", ANY_VALUE(SUBSTRING("t"."p_name", 1, 10)) AS "SHORT_NAME", COUNT(DISTINCT "supplier"."s_nationkey") AS "NATION_COUNT", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST", LISTAGG(DISTINCT "customer"."c_mktsegment", ', ') AS "MARKET_SEGMENTS", MAX("t0"."o_totalprice") AS "MAX_ORDER_PRICE"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'Z%') AS "t"
INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t0" INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."customer" ON "supplier"."s_nationkey" = "customer"."c_nationkey") ON "t0"."o_custkey" = "customer"."c_custkey") ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
GROUP BY SUBSTRING("t"."p_name", 1, 10)