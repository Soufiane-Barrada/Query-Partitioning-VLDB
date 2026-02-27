SELECT COALESCE("t11"."POSTID", "t11"."POSTID") AS "POSTID", "t11"."TITLE", "t11"."CREATIONDATE", "t11"."SCORE", "t11"."VIEWCOUNT", "t11"."OWNERDISPLAYNAME", "t11"."COMMENTCOUNT", "t11"."UPVOTES", "t11"."DOWNVOTES", "t11"."EDITCOUNT", "t11"."MOSTRECENTEDIT", "t11"."TOPUSERNAME", "t11"."TOPUSERSCORE"
FROM (SELECT "t9"."POSTID", "t9"."TITLE", "t9"."CREATIONDATE", "t9"."SCORE", "t9"."VIEWCOUNT", "t9"."OWNERDISPLAYNAME", "t9"."COMMENTCOUNT", "t9"."UPVOTES", "t9"."DOWNVOTES", "t9"."EDITCOUNT", "t9"."MOSTRECENTEDIT", "t3"."DISPLAYNAME" AS "TOPUSERNAME", "t3"."TOTALSCORE" AS "TOPUSERSCORE"
FROM (SELECT "t2"."USERID", "t2"."DisplayName" AS "DISPLAYNAME", "t2"."TOTALSCORE", "t2"."POSTCOUNT"
FROM (SELECT "Users"."Id", "Users"."DisplayName", ANY_VALUE("Users"."Id") AS "USERID", SUM("Posts"."Score") AS "TOTALSCORE", COUNT(DISTINCT "Posts"."Id") AS "POSTCOUNT"
FROM "STACK"."Users"
INNER JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
GROUP BY "Users"."Id", "Users"."DisplayName"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 5 ROWS ONLY) AS "t2") AS "t3",
(SELECT "t8"."POSTID", "t8"."TITLE", "t8"."CREATIONDATE", "t8"."SCORE", "t8"."VIEWCOUNT", "t8"."OWNERDISPLAYNAME", "t8"."COMMENTCOUNT", "t8"."UPVOTES", "t8"."DOWNVOTES", "s1"."PostId" AS "POSTID0", "s1"."EDITCOUNT", "s1"."MOSTRECENTEDIT"
FROM "s1"
RIGHT JOIN (SELECT ANY_VALUE("t5"."Id") AS "POSTID", "t5"."Title" AS "TITLE", "t5"."CreationDate" AS "CREATIONDATE", "t5"."Score" AS "SCORE", "t5"."ViewCount" AS "VIEWCOUNT", ANY_VALUE("t5"."DisplayName") AS "OWNERDISPLAYNAME", COUNT("t5"."Id1") AS "COMMENTCOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT "t4"."Id", "t4"."PostTypeId", "t4"."AcceptedAnswerId", "t4"."ParentId", "t4"."CreationDate", "t4"."Score", "t4"."ViewCount", "t4"."Body", "t4"."OwnerUserId", "t4"."OwnerDisplayName", "t4"."LastEditorUserId", "t4"."LastEditorDisplayName", "t4"."LastEditDate", "t4"."LastActivityDate", "t4"."Title", "t4"."Tags", "t4"."AnswerCount", "t4"."CommentCount", "t4"."FavoriteCount", "t4"."ClosedDate", "t4"."CommunityOwnedDate", "t4"."ContentLicense", "Users0"."Id" AS "Id0", "Users0"."Reputation", "Users0"."CreationDate" AS "CreationDate0", "Users0"."DisplayName", "Users0"."LastAccessDate", "Users0"."WebsiteUrl", "Users0"."Location", "Users0"."AboutMe", "Users0"."Views", "Users0"."UpVotes", "Users0"."DownVotes", "Users0"."ProfileImageUrl", "Users0"."AccountId", "Comments"."Id" AS "Id1", "Comments"."PostId", "Comments"."Score" AS "Score0", "Comments"."Text", "Comments"."CreationDate" AS "CreationDate1", "Comments"."UserDisplayName", "Comments"."UserId", "Comments"."ContentLicense" AS "ContentLicense0"
FROM "STACK"."Comments"
RIGHT JOIN ((SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '30' DAY)) AS "t4" INNER JOIN "STACK"."Users" AS "Users0" ON "t4"."OwnerUserId" = "Users0"."Id") ON "Comments"."PostId" = "t4"."Id") AS "t5" ON "Votes"."PostId" = "t5"."Id"
GROUP BY "t5"."Id", "t5"."Title", "t5"."CreationDate", "t5"."Score", "t5"."ViewCount", "t5"."DisplayName") AS "t8" ON "s1"."PostId" = "t8"."POSTID") AS "t9"
ORDER BY "t9"."SCORE" DESC NULLS FIRST, "t9"."VIEWCOUNT" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t11"