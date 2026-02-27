SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."PostTypeId", "t"."AcceptedAnswerId", "t"."ParentId", "t"."CreationDate", "t"."Score", "t"."ViewCount", "t"."Body", "t"."OwnerUserId", "t"."OwnerDisplayName", "t"."LastEditorUserId", "t"."LastEditorDisplayName", "t"."LastEditDate", "t"."LastActivityDate", "t"."Title", "t"."Tags", "t"."AnswerCount", "t"."CommentCount", "t"."FavoriteCount", "t"."ClosedDate", "t"."CommunityOwnedDate", "t"."ContentLicense", "t2"."USERID", "t2"."DISPLAYNAME", "t2"."REPUTATION", "t2"."UPVOTESRECEIVED", "t2"."DOWNVOTESRECEIVED"
FROM (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1 AND "Score" > 10) AS "t"
LEFT JOIN (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", "Users"."Reputation" AS "REPUTATION", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTESRECEIVED", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTESRECEIVED"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Users" ON "Votes"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName", "Users"."Reputation") AS "t2" ON "t"."OwnerUserId" = "t2"."USERID"