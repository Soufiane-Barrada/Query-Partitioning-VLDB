SELECT COALESCE("t0"."PostId", "t0"."PostId") AS "POSTID", "t0"."CLOSECOUNT", "t1"."Id", "t1"."PostTypeId", "t1"."AcceptedAnswerId", "t1"."ParentId", "t1"."CreationDate", "t1"."Score", "t1"."ViewCount", "t1"."Body", "t1"."OwnerUserId", "t1"."OwnerDisplayName", "t1"."LastEditorUserId", "t1"."LastEditorDisplayName", "t1"."LastEditDate", "t1"."LastActivityDate", "t1"."Title", "t1"."Tags", "t1"."AnswerCount", "t1"."CommentCount", "t1"."FavoriteCount", "t1"."ClosedDate", "t1"."CommunityOwnedDate", "t1"."ContentLicense", "t4"."USERID", "t4"."DISPLAYNAME", "t4"."REPUTATION", "t4"."UPVOTESRECEIVED", "t4"."DOWNVOTESRECEIVED"
FROM (SELECT "PostId", COUNT(*) AS "CLOSECOUNT"
FROM "STACK"."PostHistory"
WHERE CAST("PostHistoryTypeId" AS INTEGER) = 10
GROUP BY "PostId") AS "t0"
RIGHT JOIN ((SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1 AND "Score" > 10) AS "t1" LEFT JOIN (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", "Users"."Reputation" AS "REPUTATION", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTESRECEIVED", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTESRECEIVED"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Users" ON "Votes"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName", "Users"."Reputation") AS "t4" ON "t1"."OwnerUserId" = "t4"."USERID") ON "t0"."PostId" = "t1"."Id"