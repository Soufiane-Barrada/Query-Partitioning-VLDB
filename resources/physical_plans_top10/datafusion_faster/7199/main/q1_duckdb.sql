SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLY_VALUE"
FROM "supplier"
INNER JOIN "partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"