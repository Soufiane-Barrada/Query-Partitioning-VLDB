SELECT COALESCE("P_PARTKEY", "P_PARTKEY") AS "P_PARTKEY", "UPPERCASE_PART_NAME", "BRAND_TYPE", "MODIFIED_COMMENT", "TOTAL_SUPPLY_VALUE", "SUPPLIER_NAME", "REGION_NAME", "TOTAL_ORDERS"
FROM (SELECT "p_partkey" AS "P_PARTKEY", ANY_VALUE(UPPER("p_name")) AS "UPPERCASE_PART_NAME", ANY_VALUE(CONCAT("p_brand", ' - ', "p_type")) AS "BRAND_TYPE", ANY_VALUE(REPLACE(SUBSTRING("p_comment", 1, 15), ' ', '-')) AS "MODIFIED_COMMENT", CASE WHEN SUM("ps_supplycost" * "ps_availqty") IS NOT NULL THEN CAST(SUM("ps_supplycost" * "ps_availqty") AS DECIMAL(19, 2)) ELSE 0.00 END AS "TOTAL_SUPPLY_VALUE", ANY_VALUE("s_name") AS "SUPPLIER_NAME", ANY_VALUE("r_name") AS "REGION_NAME", COUNT(DISTINCT "o_orderkey") AS "TOTAL_ORDERS"
FROM "s1"
WHERE "p_retailprice" > 50.00
GROUP BY "p_partkey", "p_name", "p_brand", "p_type", "p_comment", "s_name", "r_name"
HAVING COUNT(DISTINCT "o_orderkey") > 10
ORDER BY 5 DESC NULLS FIRST, 2) AS "t5"