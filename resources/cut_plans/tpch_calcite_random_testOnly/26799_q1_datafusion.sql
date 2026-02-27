SELECT COALESCE("t"."n_nationkey", "t"."n_nationkey") AS "n_nationkey", "t"."n_name", "t"."n_regionkey", "t"."n_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment"
FROM (SELECT *
FROM "TPCH"."nation"
WHERE "n_name" LIKE 'A%') AS "t"
INNER JOIN "TPCH"."supplier" ON "t"."n_nationkey" = "supplier"."s_nationkey"