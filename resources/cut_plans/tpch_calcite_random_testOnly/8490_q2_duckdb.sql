SELECT COALESCE("t8"."O_ORDERKEY", "t8"."O_ORDERKEY") AS "O_ORDERKEY", "t8"."TOTAL_REVENUE", "t8"."UNIQUE_CUSTOMERS", "t8"."AVG_CUSTOMER_BALANCE", "t8"."TOTAL_SUPPLY_VALUE", "t8"."TOTAL_PARTS_SUPPLIED", "t8"."LAST_SHIP_DATE"
FROM (SELECT "t6"."O_ORDERKEY", "t6"."TOTAL_REVENUE", "t6"."UNIQUE_CUSTOMERS", "t6"."AVG_CUSTOMER_BALANCE", "s1"."TOTAL_SUPPLY_VALUE", "s1"."TOTAL_PARTS_SUPPLIED", "t6"."LAST_SHIP_DATE"
FROM "s1"
INNER JOIN (SELECT "t3"."o_orderkey" AS "O_ORDERKEY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "t3"."o_custkey") AS "UNIQUE_CUSTOMERS", AVG("customer"."c_acctbal") AS "AVG_CUSTOMER_BALANCE", MAX("lineitem"."l_shipdate") AS "LAST_SHIP_DATE", MOD("t3"."o_orderkey", 10) AS "$f5"
FROM "TPCH"."lineitem"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01') AS "t3" ON "customer"."c_custkey" = "t3"."o_custkey") ON "lineitem"."l_orderkey" = "t3"."o_orderkey"
GROUP BY "t3"."o_orderkey") AS "t6" ON "s1"."FD_COL_3" = "t6"."$f5"
ORDER BY "t6"."TOTAL_REVENUE" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t8"