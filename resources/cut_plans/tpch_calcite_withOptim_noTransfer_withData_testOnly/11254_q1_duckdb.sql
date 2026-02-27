SELECT COALESCE("t"."p_brand", "t"."p_brand") AS "P_BRAND", "t"."p_name" AS "P_NAME", "partsupp"."ps_supplycost" * "partsupp"."ps_availqty" AS "FD_COL_2", "lineitem"."l_orderkey"
FROM "TPCH"."lineitem"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" = 15) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_suppkey" = "partsupp"."ps_suppkey"