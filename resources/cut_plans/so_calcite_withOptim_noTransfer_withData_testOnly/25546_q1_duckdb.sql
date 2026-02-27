SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."Title" AS "TITLE", "t"."ViewCount" AS "VIEWCOUNT", "t"."AnswerCount" AS "ANSWERCOUNT", "t"."CreationDate" AS "CREATIONDATE", "Tags"."TagName", ', ' AS "FD_COL_6", "PostHistory"."UserId", CASE WHEN CAST("PostHistory"."PostHistoryTypeId" AS INTEGER) = 10 THEN 1 ELSE 0 END AS "FD_COL_8", CASE WHEN CAST("PostHistory"."PostHistoryTypeId" AS INTEGER) = 11 THEN 1 ELSE 0 END AS "FD_COL_9"
FROM (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1 AND "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)) AS "t"
LEFT JOIN "STACK"."Tags" ON POSITION("Tags"."TagName" IN "t"."Tags") > 0
LEFT JOIN "STACK"."PostHistory" ON "t"."Id" = "PostHistory"."PostId"