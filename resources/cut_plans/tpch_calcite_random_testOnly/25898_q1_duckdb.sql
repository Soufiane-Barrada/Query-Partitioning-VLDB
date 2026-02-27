SELECT COALESCE("part"."p_type", "part"."p_type") AS "P_TYPE", COUNT(DISTINCT "supplier"."s_name") AS "UNIQUE_SUPPLIERS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILQTY", AVG("part"."p_retailprice") AS "AVG_PRICE", LISTAGG(CONCAT("supplier"."s_name", ': ', "part"."p_name", ' - ', "part"."p_brand", ' [', "part"."p_type", ']'), ', ') AS "DESCRIPTIONS"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_type"