SELECT COALESCE(ANY_VALUE("Posts"."Id"), ANY_VALUE("Posts"."Id")) AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."Score" AS "SCORE", "Posts"."ViewCount" AS "VIEWCOUNT", ANY_VALUE("Users"."DisplayName") AS "OWNERDISPLAYNAME", COUNT("Comments"."Id") AS "COMMENTCOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES", COUNT("Badges"."Id") AS "BADGECOUNT"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
LEFT JOIN "STACK"."Badges" ON "Users"."Id" = "Badges"."UserId"
WHERE "Posts"."CreationDate" >= '2021-01-01'
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Posts"."Score", "Posts"."ViewCount", "Users"."DisplayName"