SELECT COALESCE("Users"."DisplayName", "Users"."DisplayName") AS "DISPLAYNAME", "Users"."Reputation" AS "REPUTATION", COUNT("Badges"."Id") AS "BADGECOUNT"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" ON "Badges"."UserId" = "Users"."Id"
GROUP BY "Users"."Reputation", "Users"."DisplayName"
ORDER BY "Users"."Reputation" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY