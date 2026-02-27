SELECT COALESCE(ANY_VALUE("Posts"."Id"), ANY_VALUE("Posts"."Id")) AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."ViewCount" AS "VIEWCOUNT", "Posts"."Score" AS "SCORE", ANY_VALUE("Users"."DisplayName") AS "OWNERDISPLAYNAME", COUNT("Comments"."Id") AS "COMMENTCOUNT", COUNT("Votes"."Id") AS "VOTECOUNT", AVG("Votes"."BountyAmount") AS "AVERAGEBOUNTYAMOUNT", COUNT("Badges"."Id") AS "BADGECOUNT", COUNT("PostHistory"."Id") AS "POSTHISTORYCOUNT"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
LEFT JOIN "STACK"."Badges" ON "Users"."Id" = "Badges"."UserId"
LEFT JOIN "STACK"."PostHistory" ON "Posts"."Id" = "PostHistory"."PostId"
WHERE CAST("Posts"."PostTypeId" AS INTEGER) = 1
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Posts"."ViewCount", "Posts"."Score", "Users"."DisplayName"