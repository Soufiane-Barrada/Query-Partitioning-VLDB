SELECT COALESCE("t"."r_regionkey", "t"."r_regionkey") AS "r_regionkey", "t"."r_name", "t"."r_comment", "t"."n_nationkey", "t"."n_name", "t"."n_regionkey", "t"."n_comment", CAST("t0"."s_suppkey" AS BIGINT) AS "s_suppkey", CAST("t0"."s_name" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "s_name", CAST("t0"."s_address" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "s_address", CAST("t0"."s_nationkey" AS INTEGER) AS "s_nationkey", CAST("t0"."s_phone" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "s_phone", CAST("t0"."s_acctbal" AS DECIMAL(15, 2)) AS "s_acctbal", CAST("t0"."s_comment" AS VARCHAR CHARACTER SET "ISO-8859-1") AS "s_comment"
FROM (SELECT "region"."r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment"
FROM "TPCH"."nation"
RIGHT JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey") AS "t"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t0" ON "t"."n_nationkey" = "t0"."s_nationkey"