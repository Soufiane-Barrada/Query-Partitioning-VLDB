SELECT COALESCE("t5"."SHORT_NAME", "t5"."SHORT_NAME") AS "SHORT_NAME", "t5"."NATION_COUNT", "t5"."AVG_SUPPLY_COST", "t5"."MARKET_SEGMENTS", "t5"."MAX_ORDER_PRICE"
FROM (SELECT SUBSTRING("t1"."p_name", 1, 10) AS "$f0", ANY_VALUE(SUBSTRING("t1"."p_name", 1, 10)) AS "SHORT_NAME", COUNT(DISTINCT "s1"."s_nationkey") AS "NATION_COUNT", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST", LISTAGG(DISTINCT "s1"."c_mktsegment", ', ') AS "MARKET_SEGMENTS", MAX("s1"."o_totalprice") AS "MAX_ORDER_PRICE"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'Z%') AS "t1"
INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "s1" ON "partsupp"."ps_suppkey" = "s1"."s_suppkey"
GROUP BY SUBSTRING("t1"."p_name", 1, 10)
HAVING COUNT(DISTINCT "s1"."s_nationkey") > 1
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5"