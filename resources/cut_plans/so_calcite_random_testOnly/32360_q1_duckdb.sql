SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT(DISTINCT "Badges"."Id") AS "BADGECOUNT"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" ON "Badges"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName"
HAVING COUNT(DISTINCT "Badges"."Id") > 5