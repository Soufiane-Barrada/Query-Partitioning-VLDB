SELECT COALESCE("r_regionkey", "r_regionkey") AS "r_regionkey", "r_name", "r_comment"
FROM "TPCH"."region"
WHERE "r_name" = 'Asia'