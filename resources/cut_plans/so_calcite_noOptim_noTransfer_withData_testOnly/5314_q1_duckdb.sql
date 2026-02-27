SELECT COALESCE("t2"."DISPLAYNAME", "t2"."DISPLAYNAME") AS "DISPLAYNAME", "t2"."POSTCOUNT", "t2"."POSITIVESCOREPOSTS", "t2"."NEGATIVESCOREPOSTS", "t2"."TOTALBOUNTY", "t6"."TITLE", "t6"."COMMENTCOUNT", "t6"."HIGHVIEWCOUNT", "t6"."LASTACTIVITY"
FROM (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT("Posts"."Id") AS "POSTCOUNT", SUM(CASE WHEN "Posts"."Score" > 0 THEN 1 ELSE 0 END) AS "POSITIVESCOREPOSTS", SUM(CASE WHEN "Posts"."Score" < 0 THEN 1 ELSE 0 END) AS "NEGATIVESCOREPOSTS", SUM(CASE WHEN "Votes"."BountyAmount" IS NOT NULL THEN CAST("Votes"."BountyAmount" AS INTEGER) ELSE 0 END) AS "TOTALBOUNTY"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId" AND CAST("Votes"."VoteTypeId" AS INTEGER) IN (8, 9)
WHERE "Users"."Reputation" > 1000
GROUP BY "Users"."Id", "Users"."DisplayName") AS "t2"
INNER JOIN (SELECT ANY_VALUE("Posts0"."Id") AS "POSTID", "Posts0"."Title" AS "TITLE", COUNT("Comments"."Id") AS "COMMENTCOUNT", SUM(CASE WHEN "Posts0"."ViewCount" > 100 THEN 1 ELSE 0 END) AS "HIGHVIEWCOUNT", MAX("Posts0"."CreationDate") AS "LASTACTIVITY"
FROM "STACK"."Posts" AS "Posts0"
LEFT JOIN "STACK"."Comments" ON "Posts0"."Id" = "Comments"."PostId"
WHERE "Posts0"."CreationDate" >= (CAST('2024-10-01 12:34:56' AS TIMESTAMP(0)) - INTERVAL '1' YEAR)
GROUP BY "Posts0"."Id", "Posts0"."Title") AS "t6" ON "t2"."USERID" = "t6"."POSTID"