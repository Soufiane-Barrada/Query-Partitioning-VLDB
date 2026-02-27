SELECT COALESCE("Users"."Id", "Users"."Id") AS "Id0", "Users"."DisplayName", "Badges"."Id"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" ON "Badges"."UserId" = "Users"."Id"