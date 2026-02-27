SELECT COALESCE("region"."r_regionkey", "region"."r_regionkey") AS "r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "t"."s_suppkey", "t"."s_name", "t"."s_address", "t"."s_nationkey", "t"."s_phone", "t"."s_acctbal", "t"."s_comment"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 500.00) AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"