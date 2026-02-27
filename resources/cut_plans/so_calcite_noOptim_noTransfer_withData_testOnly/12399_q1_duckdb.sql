SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", "Posts"."Score", "Posts"."OwnerUserId", "Posts"."ViewCount"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"