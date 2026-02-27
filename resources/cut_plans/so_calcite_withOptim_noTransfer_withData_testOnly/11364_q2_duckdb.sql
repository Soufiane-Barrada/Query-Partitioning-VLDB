SELECT COALESCE("s1"."POSTTYPE", "s1"."POSTTYPE") AS "POSTTYPE", "s1"."POSTCOUNT", "s1"."AVGSCORE", "s1"."TOTALVIEWS", "t4"."DisplayName" AS "DISPLAYNAME", "t4"."Reputation" AS "REPUTATION", "t4"."BADGECOUNT"
FROM "s1",
(SELECT "Users"."DisplayName", "Users"."Reputation", COUNT("Badges"."Id") AS "BADGECOUNT"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" ON "Badges"."UserId" = "Users"."Id"
GROUP BY "Users"."Reputation", "Users"."DisplayName"
ORDER BY "Users"."Reputation" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"
ORDER BY "s1"."POSTTYPE"