SELECT COALESCE("l_orderkey", "l_orderkey") AS "L_ORDERKEY", "l_partkey" AS "L_PARTKEY", "l_quantity" AS "L_QUANTITY", "l_extendedprice" AS "L_EXTENDEDPRICE", "l_discount" AS "L_DISCOUNT"
FROM "TPCH"."lineitem"
WHERE "l_returnflag" = 'N'