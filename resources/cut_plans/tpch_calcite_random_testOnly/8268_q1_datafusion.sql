SELECT COALESCE("t"."r_regionkey", "t"."r_regionkey") AS "r_regionkey", "t"."r_name", "t"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "customer"."c_custkey", "customer"."c_name", "customer"."c_address", "customer"."c_nationkey", "customer"."c_phone", "customer"."c_acctbal", "customer"."c_mktsegment", "customer"."c_comment", "t0"."o_orderkey", "t0"."o_custkey", "t0"."o_orderstatus", "t0"."o_totalprice", "t0"."o_orderdate", "t0"."o_orderpriority", "t0"."o_clerk", "t0"."o_shippriority", "t0"."o_comment"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'AMERICA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey"