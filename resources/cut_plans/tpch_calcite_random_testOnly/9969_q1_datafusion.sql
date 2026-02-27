SELECT COALESCE("region"."r_regionkey", "region"."r_regionkey") AS "r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "customer"."c_custkey", "customer"."c_name", "customer"."c_address", "customer"."c_nationkey", "customer"."c_phone", "customer"."c_acctbal", "customer"."c_mktsegment", "customer"."c_comment"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey"