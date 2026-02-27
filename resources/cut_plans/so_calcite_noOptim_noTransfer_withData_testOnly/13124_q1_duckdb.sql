SELECT COALESCE(ANY_VALUE("Posts"."Id"), ANY_VALUE("Posts"."Id")) AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."Score" AS "SCORE", "Posts"."ViewCount" AS "VIEWCOUNT", ANY_VALUE("Users"."DisplayName") AS "OWNERNAME", COUNT("Comments"."Id") AS "COMMENTCOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES", COUNT(DISTINCT "PostHistory"."Id") AS "EDITCOUNT", MIN("PostHistory"."CreationDate") AS "FIRSTEDITDATE", MAX("PostHistory"."CreationDate") AS "LASTEDITDATE"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
LEFT JOIN "STACK"."PostHistory" ON "Posts"."Id" = "PostHistory"."PostId"
WHERE CAST("Posts"."PostTypeId" AS INTEGER) = 1
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Posts"."Score", "Posts"."ViewCount", "Users"."DisplayName"