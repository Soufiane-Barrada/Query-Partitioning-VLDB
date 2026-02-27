SELECT COALESCE("Users"."Id", "Users"."Id") AS "Id", "Users"."Reputation", "Users"."CreationDate", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", "t0"."OwnerUserId" AS "OWNERUSERID", "t0"."POSTCOUNT", "t0"."TOTALVIEWS", "t0"."AVERAGESCORE"
FROM "STACK"."Users"
LEFT JOIN (SELECT "OwnerUserId", COUNT(*) AS "POSTCOUNT", SUM("ViewCount") AS "TOTALVIEWS", AVG("Score") AS "AVERAGESCORE"
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)
GROUP BY "OwnerUserId") AS "t0" ON "Users"."Id" = "t0"."OwnerUserId"