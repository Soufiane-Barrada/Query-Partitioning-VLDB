SELECT COALESCE("t1"."S_NAME", "t1"."S_NAME") AS "S_NAME", "t3"."C_NAME", "t5"."l_extendedprice" * (1 - "t5"."l_discount") AS "FD_COL_2", "t4"."o_orderkey"
FROM (SELECT "supplier"."s_suppkey" AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", DENSE_RANK() OVER (ORDER BY SUM("partsupp"."ps_availqty") DESC NULLS FIRST) AS "SUPPLIER_RANK"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name") AS "t1"
INNER JOIN ((SELECT "customer"."c_custkey" AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", DENSE_RANK() OVER (ORDER BY SUM("orders"."o_totalprice") DESC NULLS FIRST) AS "CUSTOMER_RANK"
FROM "TPCH"."orders"
RIGHT JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name") AS "t3" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t4" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t5" ON "t4"."o_orderkey" = "t5"."l_orderkey") ON "t3"."C_CUSTKEY" = "t4"."o_custkey") ON "t1"."S_SUPPKEY" = "t5"."l_suppkey"