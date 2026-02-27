SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."Title" AS "TITLE", "Comments"."Id" AS "Id0", CASE WHEN "t"."ViewCount" > 100 THEN 1 ELSE 0 END AS "FD_COL_3", "t"."CreationDate"
FROM "STACK"."Comments"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)) AS "t" ON "Comments"."PostId" = "t"."Id"