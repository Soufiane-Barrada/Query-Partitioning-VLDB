SELECT COALESCE("region"."r_regionkey", "region"."r_regionkey") AS "r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment"
FROM "TPCH"."nation"
RIGHT JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"