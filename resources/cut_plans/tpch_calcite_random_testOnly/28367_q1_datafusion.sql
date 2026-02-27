SELECT COALESCE("c_custkey", "c_custkey") AS "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"
FROM "TPCH"."customer"
WHERE "c_mktsegment" = 'BUILDING'