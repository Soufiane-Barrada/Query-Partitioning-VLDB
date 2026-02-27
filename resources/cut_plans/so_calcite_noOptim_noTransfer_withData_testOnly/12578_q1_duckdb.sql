SELECT COALESCE(ANY_VALUE("Posts"."Id"), ANY_VALUE("Posts"."Id")) AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."Score" AS "SCORE", "Posts"."ViewCount" AS "VIEWCOUNT", ANY_VALUE("Users"."DisplayName") AS "OWNERDISPLAYNAME", COUNT("Comments"."Id") AS "COMMENTCOUNT", COUNT("Votes"."Id") AS "VOTECOUNT", MAX("PostHistory"."CreationDate") AS "LASTEDITED", MAX(CASE WHEN CAST("PostHistory"."PostHistoryTypeId" AS INTEGER) = 10 THEN "PostHistory"."CreationDate" ELSE NULL END) AS "CLOSEDDATE"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
LEFT JOIN "STACK"."PostHistory" ON "Posts"."Id" = "PostHistory"."PostId"
WHERE CAST("Posts"."PostTypeId" AS INTEGER) = 1 OR CAST("Posts"."PostTypeId" AS INTEGER) = 2
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Posts"."Score", "Posts"."ViewCount", "Users"."DisplayName"