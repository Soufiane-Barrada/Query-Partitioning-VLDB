SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "part"."p_name" AS "P_NAME", "partsupp"."ps_supplycost" * "partsupp"."ps_availqty" AS "FD_COL_2", "lineitem"."l_orderkey"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."lineitem" ON "partsupp"."ps_suppkey" = "lineitem"."l_suppkey"
WHERE "part"."p_size" = 15