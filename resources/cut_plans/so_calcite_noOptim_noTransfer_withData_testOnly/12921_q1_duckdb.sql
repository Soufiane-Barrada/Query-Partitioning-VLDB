SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", "Posts"."Score", "Posts"."ViewCount", "Users"."Reputation", "Users"."Id" AS "Id1"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
LEFT JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"