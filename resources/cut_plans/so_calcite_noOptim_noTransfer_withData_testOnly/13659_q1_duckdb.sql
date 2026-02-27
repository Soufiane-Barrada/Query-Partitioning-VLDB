SELECT COALESCE(ANY_VALUE("Posts"."Id"), ANY_VALUE("Posts"."Id")) AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", COUNT(CASE WHEN "Comments"."Id" IS NOT NULL THEN 1 ELSE NULL END) AS "COMMENTCOUNT", COUNT(DISTINCT "Votes"."Id") AS "VOTECOUNT", COUNT(DISTINCT "PostHistory"."Id") AS "HISTORYCHANGECOUNT", ANY_VALUE("Users"."DisplayName") AS "OWNERDISPLAYNAME", ANY_VALUE("Users"."Reputation") AS "OWNERREPUTATION"
FROM "STACK"."Posts"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
LEFT JOIN "STACK"."PostHistory" ON "Posts"."Id" = "PostHistory"."PostId"
LEFT JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
WHERE "Posts"."CreationDate" > CAST(DATE '2020-01-01' AS TIMESTAMP(0))
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Users"."DisplayName", "Users"."Reputation"