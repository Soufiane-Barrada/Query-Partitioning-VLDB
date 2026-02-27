SELECT COALESCE("R_NAME", "R_NAME") AS "R_NAME", "REVENUE", "ORDER_COUNT", "AVG_SUPPLIER_COST"
FROM (SELECT "r_name" AS "R_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE", COUNT(DISTINCT "O_ORDERKEY") AS "ORDER_COUNT", ANY_VALUE(((SELECT AVG(SUM("ps_supplycost" * "ps_availqty"))
FROM "TPCH"."partsupp"
GROUP BY "ps_partkey"))) AS "AVG_SUPPLIER_COST"
FROM "s1"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31'
GROUP BY "r_name"
ORDER BY 2 DESC NULLS FIRST) AS "t11"