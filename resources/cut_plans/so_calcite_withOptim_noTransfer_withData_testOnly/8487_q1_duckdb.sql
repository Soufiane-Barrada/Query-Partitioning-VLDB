SELECT COALESCE("t1"."Id", "t1"."Id") AS "Id", "t1"."DisplayName" AS "DISPLAYNAME", "t1"."Id0", "t1"."VOTE_COUNT", CASE WHEN CAST("t1"."PostTypeId" AS INTEGER) = 1 THEN "t1"."AnswerCount" ELSE 0 END AS "FD_COL_4", CASE WHEN CAST("t1"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_5", "PostHistory"."Id" AS "Id1"
FROM "STACK"."PostHistory"
RIGHT JOIN (SELECT "t0"."Id", "t0"."Reputation", "t0"."CreationDate", "t0"."DisplayName", "t0"."LastAccessDate", "t0"."WebsiteUrl", "t0"."Location", "t0"."AboutMe", "t0"."Views", "t0"."UpVotes", "t0"."DownVotes", "t0"."ProfileImageUrl", "t0"."AccountId", "Posts"."Id" AS "Id0", "Posts"."PostTypeId", "Posts"."AcceptedAnswerId", "Posts"."ParentId", "Posts"."CreationDate" AS "CreationDate0", "Posts"."Score", "Posts"."ViewCount", "Posts"."Body", "Posts"."OwnerUserId", "Posts"."OwnerDisplayName", "Posts"."LastEditorUserId", "Posts"."LastEditorDisplayName", "Posts"."LastEditDate", "Posts"."LastActivityDate", "Posts"."Title", "Posts"."Tags", "Posts"."AnswerCount", "Posts"."CommentCount", "Posts"."FavoriteCount", "Posts"."ClosedDate", "Posts"."CommunityOwnedDate", "Posts"."ContentLicense", "t"."PostId" AS "POSTID", "t"."VOTE_COUNT"
FROM (SELECT "PostId", COUNT(*) AS "VOTE_COUNT"
FROM "STACK"."Votes"
GROUP BY "PostId") AS "t"
RIGHT JOIN ("STACK"."Posts" INNER JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t0" ON "Posts"."OwnerUserId" = "t0"."Id") ON "t"."PostId" = "Posts"."Id") AS "t1" ON "PostHistory"."PostId" = "t1"."Id0"