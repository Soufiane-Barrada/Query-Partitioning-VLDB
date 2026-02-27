SELECT COALESCE("t7"."POSTTYPEID", "t7"."POSTTYPEID") AS "POSTTYPEID", "t7"."TOTALPOSTS", "t7"."ACCEPTEDANSWERS", "t7"."TOTALVIEWS", "t7"."AVGSCORE", "t7"."USERID", "t7"."REPUTATION", "t7"."TOTALBADGES", "t7"."TOTALBOUNTY", "t9"."POSTHISTORYTYPE", "t9"."HISTORYCOUNT"
FROM (SELECT "t6"."POSTTYPEID", "t6"."TOTALPOSTS", "t6"."ACCEPTEDANSWERS", "t6"."TOTALVIEWS", "t6"."AVGSCORE", "t4"."USERID", "t4"."REPUTATION", "t4"."TOTALBADGES", "t4"."TOTALBOUNTY"
FROM (SELECT "t2"."USERID", "t2"."REPUTATION", "t2"."TOTALBADGES", "t2"."TOTALBOUNTY", "t"."EXPR$0"
FROM (SELECT MIN("Id") AS "EXPR$0"
FROM "STACK"."Users") AS "t",
(SELECT ANY_VALUE("t0"."Id") AS "USERID", "t0"."Reputation" AS "REPUTATION", COUNT("t0"."Id0") AS "TOTALBADGES", SUM("Votes"."BountyAmount") AS "TOTALBOUNTY"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT "Users0"."Id", "Users0"."Reputation", "Users0"."CreationDate", "Users0"."DisplayName", "Users0"."LastAccessDate", "Users0"."WebsiteUrl", "Users0"."Location", "Users0"."AboutMe", "Users0"."Views", "Users0"."UpVotes", "Users0"."DownVotes", "Users0"."ProfileImageUrl", "Users0"."AccountId", "Badges"."Id" AS "Id0", "Badges"."UserId", "Badges"."Name", "Badges"."Date", "Badges"."Class", "Badges"."TagBased"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" AS "Users0" ON "Badges"."UserId" = "Users0"."Id") AS "t0" ON "Votes"."UserId" = "t0"."Id"
GROUP BY "t0"."Id", "t0"."Reputation") AS "t2"
WHERE "t2"."USERID" = "t"."EXPR$0") AS "t4",
(SELECT "PostTypeId" AS "POSTTYPEID", COUNT(*) AS "TOTALPOSTS", SUM(1) AS "ACCEPTEDANSWERS", SUM("ViewCount") AS "TOTALVIEWS", AVG("Score") AS "AVGSCORE"
FROM "STACK"."Posts"
GROUP BY "PostTypeId") AS "t6") AS "t7",
(SELECT ANY_VALUE("PostHistoryTypes"."Name") AS "POSTHISTORYTYPE", COUNT(*) AS "HISTORYCOUNT"
FROM "STACK"."PostHistoryTypes"
INNER JOIN "STACK"."PostHistory" ON "PostHistoryTypes"."Id" = "PostHistory"."PostHistoryTypeId"
GROUP BY "PostHistoryTypes"."Name") AS "t9"