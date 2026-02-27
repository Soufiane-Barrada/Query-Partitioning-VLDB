SELECT COALESCE("l_orderkey", "l_orderkey") AS "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1998-01-01'