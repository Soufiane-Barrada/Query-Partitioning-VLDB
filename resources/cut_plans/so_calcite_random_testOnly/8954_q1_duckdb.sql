SELECT COALESCE("t1"."USERID", "t1"."USERID") AS "USERID", "t1"."DISPLAYNAME", "t1"."BADGECOUNT", "t1"."GOLDBADGES", "t1"."SILVERBADGES", "t1"."BRONZEBADGES", "t2"."Id", "t2"."Reputation", "t2"."CreationDate", "t2"."DisplayName" AS "DisplayName_", "t2"."LastAccessDate", "t2"."WebsiteUrl", "t2"."Location", "t2"."AboutMe", "t2"."Views", "t2"."UpVotes", "t2"."DownVotes", "t2"."ProfileImageUrl", "t2"."AccountId"
FROM (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT("Badges"."Id") AS "BADGECOUNT", SUM(CASE WHEN CAST("Badges"."Class" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "GOLDBADGES", SUM(CASE WHEN CAST("Badges"."Class" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "SILVERBADGES", SUM(CASE WHEN CAST("Badges"."Class" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "BRONZEBADGES"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" ON "Badges"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName") AS "t1"
INNER JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 100 AND "LastAccessDate" > (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)) AS "t2" ON "t1"."USERID" = "t2"."Id"