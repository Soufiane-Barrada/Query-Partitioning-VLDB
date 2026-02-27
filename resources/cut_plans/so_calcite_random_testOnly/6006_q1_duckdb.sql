SELECT COALESCE("t1"."OWNERUSERID", "t1"."OWNERUSERID") AS "OWNERUSERID", "t1"."TOTALPOSTS", "t1"."QUESTIONS", "t1"."ANSWERS", "t1"."TOTALSCORE", "t1"."AVGVIEWCOUNT", "t2"."Id", "t2"."Reputation", "t2"."CreationDate", "t2"."DisplayName", "t2"."LastAccessDate", "t2"."WebsiteUrl", "t2"."Location", "t2"."AboutMe", "t2"."Views", "t2"."UpVotes", "t2"."DownVotes", "t2"."ProfileImageUrl", "t2"."AccountId"
FROM (SELECT "OwnerUserId" AS "OWNERUSERID", COUNT(*) AS "TOTALPOSTS", SUM(CASE WHEN CAST("PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONS", SUM(CASE WHEN CAST("PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERS", SUM("Score") AS "TOTALSCORE", AVG("ViewCount") AS "AVGVIEWCOUNT"
FROM "STACK"."Posts"
WHERE "CreationDate" > CAST((CURRENT_DATE - INTERVAL '1' YEAR) AS TIMESTAMP(0))
GROUP BY "OwnerUserId") AS "t1"
INNER JOIN (SELECT *
FROM "STACK"."Users"
WHERE "LastAccessDate" > CAST((CURRENT_DATE - INTERVAL '6' MONTH) AS TIMESTAMP(0))) AS "t2" ON "t1"."OWNERUSERID" = "t2"."Id"