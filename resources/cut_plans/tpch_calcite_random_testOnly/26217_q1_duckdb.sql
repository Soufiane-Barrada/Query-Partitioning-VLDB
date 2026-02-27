SELECT COALESCE("region"."r_regionkey", "region"."r_regionkey") AS "r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"