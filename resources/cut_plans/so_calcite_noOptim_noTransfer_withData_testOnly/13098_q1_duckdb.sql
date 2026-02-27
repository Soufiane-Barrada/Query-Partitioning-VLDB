SELECT COALESCE(ANY_VALUE("Users"."DisplayName"), ANY_VALUE("Users"."DisplayName")) AS "USERDISPLAYNAME", "Users"."Reputation" AS "REPUTATION", COUNT("Posts"."Id") AS "TOTALPOSTS", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONSCOUNT", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERSCOUNT", SUM("Posts"."ViewCount") AS "TOTALVIEWS", SUM("Posts"."Score") AS "TOTALSCORE", AVG(CASE WHEN LENGTH("Posts"."Body") IS NOT NULL THEN CAST(LENGTH("Posts"."Body") AS INTEGER) ELSE 0 END) AS "AVERAGEPOSTLENGTH", COUNT("Comments"."Id") AS "TOTALCOMMENTS", COUNT("Badges"."Id") AS "TOTALBADGES"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Badges" ON "Users"."Id" = "Badges"."UserId"
WHERE "Users"."CreationDate" < (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)
GROUP BY "Users"."DisplayName", "Users"."Reputation"