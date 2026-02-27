SELECT COALESCE("region"."r_regionkey", "region"."r_regionkey") AS "r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"