SELECT COALESCE("t10"."DISPLAYNAME", "t10"."DISPLAYNAME") AS "DISPLAYNAME", "t10"."POSTCOUNT", "t10"."TOTALVOTES", "t10"."QUESTIONSANSWERED", "t10"."ANSWERSGIVEN", "t10"."POSTEDITS", "t10"."POSTTYPE", "t10"."TOTALPOSTSOFTYPE", "t10"."AVERAGESCORE", "t10"."TOTALVIEWS"
FROM (SELECT "t8"."DISPLAYNAME", "t8"."POSTCOUNT", "t8"."TOTALVOTES", "t8"."QUESTIONSANSWERED", "t8"."ANSWERSGIVEN", "t8"."POSTEDITS", "t3"."POSTTYPE", "t3"."POSTCOUNT" AS "TOTALPOSTSOFTYPE", "t3"."AVERAGESCORE", "t3"."TOTALVIEWS"
FROM (SELECT ANY_VALUE("PostTypes"."Name") AS "POSTTYPE", COUNT(*) AS "POSTCOUNT", AVG("t1"."Score") AS "AVERAGESCORE", SUM("t1"."ViewCount") AS "TOTALVIEWS"
FROM "STACK"."PostTypes"
INNER JOIN (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)) AS "t1" ON "PostTypes"."Id" = "t1"."PostTypeId"
GROUP BY "PostTypes"."Name") AS "t3",
(SELECT ANY_VALUE("t5"."Id") AS "USERID", "t5"."DisplayName" AS "DISPLAYNAME", COUNT(DISTINCT "t5"."Id0") AS "POSTCOUNT", SUM("t5"."VOTE_COUNT") AS "TOTALVOTES", SUM(CASE WHEN CAST("t5"."PostTypeId" AS INTEGER) = 1 THEN "t5"."AnswerCount" ELSE 0 END) AS "QUESTIONSANSWERED", SUM(CASE WHEN CAST("t5"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERSGIVEN", COUNT(DISTINCT "PostHistory"."Id") AS "POSTEDITS"
FROM "STACK"."PostHistory"
RIGHT JOIN (SELECT "t4"."Id", "t4"."Reputation", "t4"."CreationDate", "t4"."DisplayName", "t4"."LastAccessDate", "t4"."WebsiteUrl", "t4"."Location", "t4"."AboutMe", "t4"."Views", "t4"."UpVotes", "t4"."DownVotes", "t4"."ProfileImageUrl", "t4"."AccountId", "Posts0"."Id" AS "Id0", "Posts0"."PostTypeId", "Posts0"."AcceptedAnswerId", "Posts0"."ParentId", "Posts0"."CreationDate" AS "CreationDate0", "Posts0"."Score", "Posts0"."ViewCount", "Posts0"."Body", "Posts0"."OwnerUserId", "Posts0"."OwnerDisplayName", "Posts0"."LastEditorUserId", "Posts0"."LastEditorDisplayName", "Posts0"."LastEditDate", "Posts0"."LastActivityDate", "Posts0"."Title", "Posts0"."Tags", "Posts0"."AnswerCount", "Posts0"."CommentCount", "Posts0"."FavoriteCount", "Posts0"."ClosedDate", "Posts0"."CommunityOwnedDate", "Posts0"."ContentLicense", "s1"."PostId" AS "POSTID", "s1"."VOTE_COUNT"
FROM "s1"
RIGHT JOIN ("STACK"."Posts" AS "Posts0" INNER JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t4" ON "Posts0"."OwnerUserId" = "t4"."Id") ON "s1"."PostId" = "Posts0"."Id") AS "t5" ON "PostHistory"."PostId" = "t5"."Id0"
GROUP BY "t5"."Id", "t5"."DisplayName") AS "t8"
ORDER BY "t8"."TOTALVOTES" DESC NULLS FIRST, "t8"."POSTCOUNT" DESC NULLS FIRST, "t3"."TOTALVIEWS" DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t10"