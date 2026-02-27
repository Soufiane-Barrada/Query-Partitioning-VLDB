SELECT COALESCE("PART_SUPPLIER", "PART_SUPPLIER") AS "PART_SUPPLIER", "UNIQUE_CUSTOMERS", "TOTAL_REVENUE", "REGION_NAME", "COMMENT_EXCERPT"
FROM (SELECT ANY_VALUE(CONCAT("p_name", ' - ', "s_name")) AS "PART_SUPPLIER", COUNT(DISTINCT "c_custkey") AS "UNIQUE_CUSTOMERS", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("r_name") AS "REGION_NAME", ANY_VALUE(LEFT("p_comment", 10)) AS "COMMENT_EXCERPT"
FROM "s1"
WHERE "l_shipdate" >= '1997-01-01' AND "l_shipdate" <= '1997-12-31' AND "o_orderstatus" = 'F' AND ("p_container" = 'BOX' OR "p_container" = 'PACK')
GROUP BY "p_name", "s_name", "r_name", "p_comment"
HAVING SUM("l_extendedprice" * (1 - "l_discount")) > 100000.0000
ORDER BY 3 DESC NULLS FIRST, 2) AS "t5"