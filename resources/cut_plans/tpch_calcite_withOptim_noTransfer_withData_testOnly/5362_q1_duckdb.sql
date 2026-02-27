SELECT COALESCE("t1"."O_ORDERKEY", "t1"."O_ORDERKEY") AS "O_ORDERKEY", "t1"."O_ORDERDATE", "t1"."TOTAL_REVENUE", "customer"."c_custkey", "customer"."c_name", "customer"."c_address", "customer"."c_nationkey", "customer"."c_phone", "customer"."c_acctbal", "customer"."c_mktsegment", "customer"."c_comment", "orders0"."o_orderkey" AS "o_orderkey_", "orders0"."o_custkey", "orders0"."o_orderstatus", "orders0"."o_totalprice", "orders0"."o_orderdate" AS "o_orderdate_", "orders0"."o_orderpriority", "orders0"."o_clerk", "orders0"."o_shippriority", "orders0"."o_comment"
FROM (SELECT "t"."o_orderkey" AS "O_ORDERKEY", "t"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "t"."o_orderkey", "t"."o_orderdate") AS "t1"
INNER JOIN ("TPCH"."customer" INNER JOIN "TPCH"."orders" AS "orders0" ON "customer"."c_custkey" = "orders0"."o_custkey") ON "t1"."O_ORDERKEY" = "orders0"."o_orderkey"