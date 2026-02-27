SELECT COALESCE("t2"."POSTID", "t2"."POSTID") AS "POSTID", "t2"."TITLE", "t2"."OWNER", "t2"."CREATIONDATE", "t2"."SCORE", "t2"."VIEWCOUNT", "t2"."ANSWERCOUNT", "t2"."COMMENTCOUNT"
FROM (SELECT "s1"."Id" AS "POSTID", "s1"."Title" AS "TITLE", "Users"."DisplayName" AS "OWNER", "s1"."CreationDate" AS "CREATIONDATE", "s1"."Score" AS "SCORE", "s1"."ViewCount" AS "VIEWCOUNT", "s1"."AnswerCount" AS "ANSWERCOUNT", "s1"."CommentCount" AS "COMMENTCOUNT"
FROM "s1"
INNER JOIN "STACK"."Users" ON "s1"."OwnerUserId" = "Users"."Id"
ORDER BY "s1"."CreationDate" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"