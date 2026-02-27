SELECT COALESCE("t0"."Id", "t0"."Id") AS "Id", "t0"."PostTypeId", "t0"."AcceptedAnswerId", "t0"."ParentId", "t0"."CreationDate", "t0"."Score", "t0"."ViewCount", "t0"."Body", "t0"."OwnerUserId", "t0"."OwnerDisplayName", "t0"."LastEditorUserId", "t0"."LastEditorDisplayName", "t0"."LastEditDate", "t0"."LastActivityDate", "t0"."Title", "t0"."Tags", "t0"."AnswerCount", "t0"."CommentCount", "t0"."FavoriteCount", "t0"."ClosedDate", "t0"."CommunityOwnedDate", "t0"."ContentLicense", "Users"."Id" AS "Id0", "Users"."Reputation", "Users"."CreationDate" AS "CreationDate0", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", "t"."PostId" AS "POSTID", "t"."COMMENTCOUNT" AS "COMMENTCOUNT_"
FROM (SELECT "PostId", COUNT(*) AS "COMMENTCOUNT"
FROM "STACK"."Comments"
GROUP BY "PostId") AS "t"
RIGHT JOIN ("STACK"."Users" INNER JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t0" ON "Users"."Id" = "t0"."OwnerUserId") ON "t"."PostId" = "t0"."Id"