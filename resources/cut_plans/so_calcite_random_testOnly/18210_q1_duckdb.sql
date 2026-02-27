SELECT COALESCE("Users"."DisplayName", "Users"."DisplayName") AS "DisplayName", COUNT(*) AS "POSTCOUNT", SUM("Posts"."ViewCount") AS "TOTALVIEWS"
FROM "STACK"."Users"
INNER JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
GROUP BY "Users"."DisplayName"