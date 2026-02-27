SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", "Posts"."Score", "Posts"."OwnerUserId"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"