SELECT COALESCE("t0"."DisplayName", "t0"."DisplayName") AS "DISPLAYNAME", "t0"."BADGECOUNT", "t0"."MAXREPUTATION", "t0"."AVGREPUTATION", "t2"."POSTTYPE", "t2"."POSTCOUNT", "t2"."AVGSCORE", "t2"."TOTALVIEWS", "t2"."AVGANSWERCOUNT", "t2"."AVGCOMMENTCOUNT"
FROM (SELECT "Users"."DisplayName", COUNT("Badges"."Id") AS "BADGECOUNT", MAX("Users"."Reputation") AS "MAXREPUTATION", AVG("Users"."Reputation") AS "AVGREPUTATION"
FROM "STACK"."Badges"
RIGHT JOIN "STACK"."Users" ON "Badges"."UserId" = "Users"."Id"
GROUP BY "Users"."DisplayName"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 1 ROWS ONLY) AS "t0",
(SELECT ANY_VALUE("PostTypes"."Name") AS "POSTTYPE", COUNT(*) AS "POSTCOUNT", AVG("Posts"."Score") AS "AVGSCORE", SUM("Posts"."ViewCount") AS "TOTALVIEWS", AVG("Posts"."AnswerCount") AS "AVGANSWERCOUNT", AVG("Posts"."CommentCount") AS "AVGCOMMENTCOUNT"
FROM "STACK"."PostTypes"
INNER JOIN "STACK"."Posts" ON "PostTypes"."Id" = "Posts"."PostTypeId"
GROUP BY "PostTypes"."Name") AS "t2"