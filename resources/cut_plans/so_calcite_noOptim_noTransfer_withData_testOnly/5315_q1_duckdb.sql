SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."CREATIONDATE", "Users"."DisplayName" AS "OWNERDISPLAYNAME", "t2"."SCORE", "t2"."UPVOTES", "t2"."DOWNVOTES", "t5"."TOTALBADGES", "t5"."TOTALPOSTS"
FROM (SELECT ANY_VALUE("Posts"."Id") AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."Score" AS "SCORE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."OwnerUserId" AS "OWNERUSERID", "Posts"."AnswerCount" AS "ANSWERCOUNT", "Posts"."CommentCount" AS "COMMENTCOUNT", COUNT(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE NULL END) AS "UPVOTES", COUNT(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE NULL END) AS "DOWNVOTES"
FROM "STACK"."Posts"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
WHERE "Posts"."CreationDate" > (CAST('2024-10-01 12:34:56' AS TIMESTAMP(0)) - INTERVAL '30' DAY)
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."Score", "Posts"."CreationDate", "Posts"."OwnerUserId", "Posts"."AnswerCount", "Posts"."CommentCount") AS "t2"
INNER JOIN "STACK"."Users" ON "t2"."OWNERUSERID" = "Users"."Id"
INNER JOIN (SELECT "Users0"."Id" AS "ID", "Users0"."DisplayName" AS "DISPLAYNAME", SUM(CASE WHEN "Badges"."Class" IS NOT NULL THEN CAST("Badges"."Class" AS INTEGER) ELSE 0 END) AS "TOTALBADGES", COUNT(DISTINCT "Posts0"."Id") AS "TOTALPOSTS"
FROM "STACK"."Users" AS "Users0"
LEFT JOIN "STACK"."Badges" ON "Users0"."Id" = "Badges"."UserId"
LEFT JOIN "STACK"."Posts" AS "Posts0" ON "Users0"."Id" = "Posts0"."OwnerUserId"
GROUP BY "Users0"."Id", "Users0"."DisplayName"
ORDER BY 4 DESC NULLS FIRST, 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5" ON "Users"."Id" = "t5"."ID"