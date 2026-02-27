SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "partsupp"."ps_suppkey"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"