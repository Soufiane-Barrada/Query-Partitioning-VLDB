SELECT COALESCE("Badges"."Id", "Badges"."Id") AS "Id", "Badges"."UserId", "Badges"."Name", "Badges"."Date", "Badges"."Class", "Badges"."TagBased", "t5"."USERID" AS "USERID_", "t5"."DISPLAYNAME", "t5"."POSTCOUNT", "t5"."QUESTIONS", "t5"."ANSWERS", "t5"."CLOSEDPOSTS", "t5"."AVGREPUTATION", "t5"."NETVOTES"
FROM "STACK"."Badges"
RIGHT JOIN (SELECT ANY_VALUE("t0"."Id") AS "USERID", "t0"."DisplayName" AS "DISPLAYNAME", COUNT(DISTINCT "t0"."Id0") AS "POSTCOUNT", SUM(CASE WHEN CAST("t0"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONS", SUM(CASE WHEN CAST("t0"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERS", SUM(CASE WHEN CAST("t0"."PostTypeId" AS INTEGER) IN (10, 11) THEN 1 ELSE 0 END) AS "CLOSEDPOSTS", AVG("t0"."Reputation") AS "AVGREPUTATION", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) - SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "NETVOTES"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT "t"."Id", "t"."Reputation", "t"."CreationDate", "t"."DisplayName", "t"."LastAccessDate", "t"."WebsiteUrl", "t"."Location", "t"."AboutMe", "t"."Views", "t"."UpVotes", "t"."DownVotes", "t"."ProfileImageUrl", "t"."AccountId", "Posts"."Id" AS "Id0", "Posts"."PostTypeId", "Posts"."AcceptedAnswerId", "Posts"."ParentId", "Posts"."CreationDate" AS "CreationDate0", "Posts"."Score", "Posts"."ViewCount", "Posts"."Body", "Posts"."OwnerUserId", "Posts"."OwnerDisplayName", "Posts"."LastEditorUserId", "Posts"."LastEditorDisplayName", "Posts"."LastEditDate", "Posts"."LastActivityDate", "Posts"."Title", "Posts"."Tags", "Posts"."AnswerCount", "Posts"."CommentCount", "Posts"."FavoriteCount", "Posts"."ClosedDate", "Posts"."CommunityOwnedDate", "Posts"."ContentLicense"
FROM "STACK"."Posts"
RIGHT JOIN (SELECT *
FROM "STACK"."Users"
WHERE "CreationDate" < (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)) AS "t" ON "Posts"."OwnerUserId" = "t"."Id") AS "t0" ON "Votes"."PostId" = "t0"."Id0"
GROUP BY "t0"."Id", "t0"."DisplayName"
HAVING COUNT(DISTINCT "t0"."Id0") > 5
ORDER BY 8 DESC NULLS FIRST, 7 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5" ON "Badges"."UserId" = "t5"."USERID"