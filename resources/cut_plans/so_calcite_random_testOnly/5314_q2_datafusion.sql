SELECT COALESCE("t11"."DISPLAYNAME", "t11"."DISPLAYNAME") AS "DISPLAYNAME", "t11"."POSTCOUNT", "t11"."POSITIVESCOREPOSTS", "t11"."NEGATIVESCOREPOSTS", "t11"."TOTALBOUNTY", "t11"."TITLE", "t11"."COMMENTCOUNT", "t11"."HIGHVIEWCOUNT", "t11"."LASTACTIVITY"
FROM (SELECT "t9"."DISPLAYNAME", "t9"."POSTCOUNT", "t9"."POSITIVESCOREPOSTS", "t9"."NEGATIVESCOREPOSTS", "t9"."TOTALBOUNTY", "t3"."TITLE", "t3"."COMMENTCOUNT", "t3"."HIGHVIEWCOUNT", "t3"."LASTACTIVITY"
FROM (SELECT ANY_VALUE("Id") AS "POSTID", "TITLE", COUNT("Id0") AS "COMMENTCOUNT", SUM("FD_COL_3") AS "HIGHVIEWCOUNT", MAX("CreationDate") AS "LASTACTIVITY"
FROM "s1"
GROUP BY "Id", "TITLE") AS "t3"
INNER JOIN (SELECT ANY_VALUE("t5"."Id") AS "USERID", "t5"."DisplayName" AS "DISPLAYNAME", COUNT("t5"."Id0") AS "POSTCOUNT", SUM(CASE WHEN "t5"."Score" > 0 THEN 1 ELSE 0 END) AS "POSITIVESCOREPOSTS", SUM(CASE WHEN "t5"."Score" < 0 THEN 1 ELSE 0 END) AS "NEGATIVESCOREPOSTS", SUM(CASE WHEN "t6"."BountyAmount" IS NOT NULL THEN CAST("t6"."BountyAmount" AS INTEGER) ELSE 0 END) AS "TOTALBOUNTY"
FROM (SELECT "t4"."Id", "t4"."Reputation", "t4"."CreationDate", "t4"."DisplayName", "t4"."LastAccessDate", "t4"."WebsiteUrl", "t4"."Location", "t4"."AboutMe", "t4"."Views", "t4"."UpVotes", "t4"."DownVotes", "t4"."ProfileImageUrl", "t4"."AccountId", "Posts0"."Id" AS "Id0", "Posts0"."PostTypeId", "Posts0"."AcceptedAnswerId", "Posts0"."ParentId", "Posts0"."CreationDate" AS "CreationDate0", "Posts0"."Score", "Posts0"."ViewCount", "Posts0"."Body", "Posts0"."OwnerUserId", "Posts0"."OwnerDisplayName", "Posts0"."LastEditorUserId", "Posts0"."LastEditorDisplayName", "Posts0"."LastEditDate", "Posts0"."LastActivityDate", "Posts0"."Title", "Posts0"."Tags", "Posts0"."AnswerCount", "Posts0"."CommentCount", "Posts0"."FavoriteCount", "Posts0"."ClosedDate", "Posts0"."CommunityOwnedDate", "Posts0"."ContentLicense"
FROM "STACK"."Posts" AS "Posts0"
RIGHT JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t4" ON "Posts0"."OwnerUserId" = "t4"."Id") AS "t5"
LEFT JOIN (SELECT *
FROM "STACK"."Votes"
WHERE CAST("VoteTypeId" AS INTEGER) IN (8, 9)) AS "t6" ON "t5"."Id0" = "t6"."PostId"
GROUP BY "t5"."Id", "t5"."DisplayName") AS "t9" ON "t3"."POSTID" = "t9"."USERID"
ORDER BY "t9"."TOTALBOUNTY" DESC NULLS FIRST, "t9"."POSTCOUNT" DESC NULLS FIRST
FETCH NEXT 50 ROWS ONLY) AS "t11"