SELECT COALESCE("t1"."p_brand", "t1"."p_brand") AS "p_brand", "t1"."p_type", "t1"."TOTAL_AVAILABLE_QTY", "t1"."AVG_SUPPLY_COST"
FROM (SELECT "part"."p_brand", "part"."p_type", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QTY", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "s1"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_brand", "part"."p_type"
ORDER BY 3 DESC NULLS FIRST, 4
FETCH NEXT 100 ROWS ONLY) AS "t1"