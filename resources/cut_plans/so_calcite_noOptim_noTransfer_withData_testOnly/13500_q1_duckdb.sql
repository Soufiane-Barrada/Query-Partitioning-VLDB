SELECT COALESCE(ANY_VALUE("Posts"."Id"), ANY_VALUE("Posts"."Id")) AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."ViewCount" AS "VIEWCOUNT", "Posts"."Score" AS "SCORE", COUNT("Comments"."Id") AS "COMMENTCOUNT", COUNT(DISTINCT "Votes"."Id") AS "VOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT", ANY_VALUE("Users"."DisplayName") AS "OWNERDISPLAYNAME", ANY_VALUE("PostTypes"."Name") AS "POSTTYPENAME"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
WHERE "Posts"."CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Posts"."ViewCount", "Posts"."Score", "Users"."DisplayName", "PostTypes"."Name"