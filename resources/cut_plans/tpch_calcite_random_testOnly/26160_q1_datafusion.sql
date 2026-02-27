SELECT COALESCE("t0"."n_nationkey", "t0"."n_nationkey") AS "N_NATIONKEY", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment"
FROM (SELECT "n_nationkey"
FROM "TPCH"."nation"
WHERE "n_name" = 'Germany'
GROUP BY "n_nationkey") AS "t0"
INNER JOIN "TPCH"."supplier" ON "t0"."n_nationkey" = "supplier"."s_nationkey"