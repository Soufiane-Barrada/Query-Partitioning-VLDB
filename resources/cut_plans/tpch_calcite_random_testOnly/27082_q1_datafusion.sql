SELECT COALESCE("n_nationkey", "n_nationkey") AS "n_nationkey", "n_name", "n_regionkey", "n_comment"
FROM "TPCH"."nation"
WHERE "n_name" = 'USA'