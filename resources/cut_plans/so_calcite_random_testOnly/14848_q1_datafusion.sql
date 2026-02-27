SELECT COALESCE(ANY_VALUE("t"."Id"), ANY_VALUE("t"."Id")) AS "USERID", "t"."Reputation" AS "REPUTATION", COUNT(DISTINCT "t"."Id0") AS "POSTSCOUNT", SUM("t0"."BountyAmount") AS "TOTALBOUNTIES", COUNT(DISTINCT "t"."Id0") > 0 AS "FD_COL_4"
FROM (SELECT "Users"."Id", "Users"."Reputation", "Users"."CreationDate", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", "Posts"."Id" AS "Id0", "Posts"."PostTypeId", "Posts"."AcceptedAnswerId", "Posts"."ParentId", "Posts"."CreationDate" AS "CreationDate0", "Posts"."Score", "Posts"."ViewCount", "Posts"."Body", "Posts"."OwnerUserId", "Posts"."OwnerDisplayName", "Posts"."LastEditorUserId", "Posts"."LastEditorDisplayName", "Posts"."LastEditDate", "Posts"."LastActivityDate", "Posts"."Title", "Posts"."Tags", "Posts"."AnswerCount", "Posts"."CommentCount", "Posts"."FavoriteCount", "Posts"."ClosedDate", "Posts"."CommunityOwnedDate", "Posts"."ContentLicense"
FROM "STACK"."Posts"
RIGHT JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id") AS "t"
LEFT JOIN (SELECT *
FROM "STACK"."Votes"
WHERE CAST("VoteTypeId" AS INTEGER) = 8) AS "t0" ON "t"."Id0" = "t0"."PostId"
GROUP BY "t"."Id", "t"."Reputation"
HAVING COUNT(DISTINCT "t"."Id0") > 0