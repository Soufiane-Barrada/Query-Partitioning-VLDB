SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", "partsupp"."ps_supplycost" * "partsupp"."ps_availqty" AS "FD_COL_2", "partsupp"."ps_partkey"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"