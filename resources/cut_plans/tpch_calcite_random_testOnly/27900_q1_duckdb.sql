SELECT COALESCE("t"."l_orderkey", "t"."l_orderkey") AS "l_orderkey", "t"."l_partkey", "t"."l_suppkey", "t"."l_linenumber", "t"."l_quantity", "t"."l_extendedprice", "t"."l_discount", "t"."l_tax", "t"."l_returnflag", "t"."l_linestatus", "t"."l_shipdate", "t"."l_commitdate", "t"."l_receiptdate", "t"."l_shipinstruct", "t"."l_shipmode", "t"."l_comment", "orders"."o_orderkey", "orders"."o_custkey", "orders"."o_orderstatus", "orders"."o_totalprice", "orders"."o_orderdate", "orders"."o_orderpriority", "orders"."o_clerk", "orders"."o_shippriority", "orders"."o_comment"
FROM (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t"
INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey"