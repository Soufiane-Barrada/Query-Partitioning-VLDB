SELECT COALESCE("s_suppkey", "s_suppkey") AS "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"
FROM "TPCH"."supplier"
WHERE "s_name" LIKE 'Supplier%'