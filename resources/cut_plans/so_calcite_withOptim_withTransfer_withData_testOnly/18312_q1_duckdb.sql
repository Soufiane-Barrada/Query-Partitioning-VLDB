SELECT COALESCE("t"."Id", "t"."Id") AS "Id"
FROM (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t"
INNER JOIN "STACK"."Users" ON "t"."OwnerUserId" = "Users"."Id"