SELECT COALESCE(ANY_VALUE("Posts"."Title"), ANY_VALUE("Posts"."Title")) AS "POSTTITLE", ANY_VALUE("Posts"."CreationDate") AS "POSTCREATIONDATE", ANY_VALUE("Users"."DisplayName") AS "AUTHORDISPLAYNAME", COUNT("Comments"."Id") AS "COMMENTCOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES", "Posts"."ViewCount" AS "VIEWCOUNT", "Posts"."Score" AS "SCORE", ANY_VALUE("PostTypes"."Name") AS "POSTTYPENAME", "Posts"."CreationDate"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
WHERE "Posts"."CreationDate" >= '2022-01-01'
GROUP BY "Posts"."Title", "Posts"."CreationDate", "Users"."DisplayName", "Posts"."ViewCount", "Posts"."Score", "PostTypes"."Name"