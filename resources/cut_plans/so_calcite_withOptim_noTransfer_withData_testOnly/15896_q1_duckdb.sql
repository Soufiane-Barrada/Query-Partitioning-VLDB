SELECT COALESCE("t3"."Id", "t3"."Id") AS "Id", "t3"."PostTypeId", "t3"."AcceptedAnswerId", "t3"."ParentId", "t3"."CreationDate", "t3"."Score", "t3"."ViewCount", "t3"."Body", "t3"."OwnerUserId", "t3"."OwnerDisplayName", "t3"."LastEditorUserId", "t3"."LastEditorDisplayName", "t3"."LastEditDate", "t3"."LastActivityDate", "t3"."Title", "t3"."Tags", "t3"."AnswerCount", "t3"."CommentCount", "t3"."FavoriteCount", "t3"."ClosedDate", "t3"."CommunityOwnedDate", "t3"."ContentLicense", "t3"."Id0", "t3"."Reputation", "t3"."CreationDate0", "t3"."DisplayName", "t3"."LastAccessDate", "t3"."WebsiteUrl", "t3"."Location", "t3"."AboutMe", "t3"."Views", "t3"."UpVotes", "t3"."DownVotes", "t3"."ProfileImageUrl", "t3"."AccountId", "t3"."POSTID", "t3"."COMMENTCOUNT" AS "COMMENTCOUNT_", "t0"."ParentId" AS "PARENTID_", "t0"."ANSWERCOUNT" AS "ANSWERCOUNT_"
FROM (SELECT "ParentId", COUNT(*) AS "ANSWERCOUNT"
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 2
GROUP BY "ParentId") AS "t0"
RIGHT JOIN (SELECT "t2"."Id", "t2"."PostTypeId", "t2"."AcceptedAnswerId", "t2"."ParentId", "t2"."CreationDate", "t2"."Score", "t2"."ViewCount", "t2"."Body", "t2"."OwnerUserId", "t2"."OwnerDisplayName", "t2"."LastEditorUserId", "t2"."LastEditorDisplayName", "t2"."LastEditDate", "t2"."LastActivityDate", "t2"."Title", "t2"."Tags", "t2"."AnswerCount", "t2"."CommentCount", "t2"."FavoriteCount", "t2"."ClosedDate", "t2"."CommunityOwnedDate", "t2"."ContentLicense", "Users"."Id" AS "Id0", "Users"."Reputation", "Users"."CreationDate" AS "CreationDate0", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", "t1"."PostId" AS "POSTID", "t1"."COMMENTCOUNT"
FROM (SELECT "PostId", COUNT(*) AS "COMMENTCOUNT"
FROM "STACK"."Comments"
GROUP BY "PostId") AS "t1"
RIGHT JOIN ("STACK"."Users" INNER JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t2" ON "Users"."Id" = "t2"."OwnerUserId") ON "t1"."PostId" = "t2"."Id") AS "t3" ON "t0"."ParentId" = "t3"."Id"