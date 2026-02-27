SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", "Users"."Reputation" AS "REPUTATION", COUNT("Posts"."Id") AS "POSTCOUNT", COUNT("Comments"."Id") AS "COMMENTCOUNT", SUM(CASE WHEN "Votes"."CreationDate" IS NOT NULL THEN 1 ELSE 0 END) AS "VOTECOUNT", SUM(CASE WHEN "Badges"."Id" IS NOT NULL THEN 1 ELSE 0 END) AS "BADGECOUNT"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Users"."Id" = "Votes"."UserId"
LEFT JOIN "STACK"."Badges" ON "Users"."Id" = "Badges"."UserId"
WHERE "Users"."Reputation" > 0
GROUP BY "Users"."Id", "Users"."DisplayName", "Users"."Reputation"