SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "s_suppkey", "supplier"."s_name", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLYCOST"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"