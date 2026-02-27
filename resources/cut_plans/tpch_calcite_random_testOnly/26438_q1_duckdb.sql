SELECT COALESCE("o_orderkey", "o_orderkey") AS "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1994-01-01' AND "o_orderdate" <= DATE '1994-12-31'