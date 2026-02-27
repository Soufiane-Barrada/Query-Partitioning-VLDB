SELECT COALESCE("t"."UserId", "t"."UserId") AS "USERID", "t"."BADGECOUNT", "t"."HIGHESTBADGECLASS", "t0"."Id", "t0"."Reputation", "t0"."CreationDate", "t0"."DisplayName", "t0"."LastAccessDate", "t0"."WebsiteUrl", "t0"."Location", "t0"."AboutMe", "t0"."Views", "t0"."UpVotes", "t0"."DownVotes", "t0"."ProfileImageUrl", "t0"."AccountId", "t2"."OWNERUSERID", "t2"."POSTCOUNT", "t2"."QUESTIONCOUNT", "t2"."ANSWERCOUNT", "t2"."AVGSCORE", "t2"."TOTALVIEWS", "t2"."TOTALCOMMENTS"
FROM (SELECT "UserId", COUNT(*) AS "BADGECOUNT", MAX("Class") AS "HIGHESTBADGECLASS"
FROM "STACK"."Badges"
GROUP BY "UserId") AS "t"
RIGHT JOIN ((SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t0" INNER JOIN (SELECT "OwnerUserId" AS "OWNERUSERID", COUNT(*) AS "POSTCOUNT", SUM(CASE WHEN CAST("PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONCOUNT", SUM(CASE WHEN CAST("PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERCOUNT", AVG("Score") AS "AVGSCORE", SUM("ViewCount") AS "TOTALVIEWS", SUM("CommentCount") AS "TOTALCOMMENTS"
FROM "STACK"."Posts"
GROUP BY "OwnerUserId") AS "t2" ON "t0"."Id" = "t2"."OWNERUSERID") ON "t"."UserId" = "t0"."Id"