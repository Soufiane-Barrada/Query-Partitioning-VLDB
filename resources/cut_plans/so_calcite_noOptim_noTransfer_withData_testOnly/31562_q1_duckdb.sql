SELECT COALESCE("Users"."Id", "Users"."Id") AS "Id", "Users"."DisplayName" AS "DISPLAYNAME", "Users"."Reputation" AS "REPUTATION", "Posts"."Id" AS "Id0", "Posts0"."Id" AS "Id1", CASE WHEN "Posts0"."AcceptedAnswerId" IS NOT NULL THEN 1 ELSE 0 END AS "FD_COL_5", CASE WHEN "t0"."COMMENT_COUNT" IS NOT NULL THEN CAST("t0"."COMMENT_COUNT" AS BIGINT) ELSE 0 END AS "FD_COL_6", CASE WHEN "t2"."UPVOTE_COUNT" IS NOT NULL THEN CAST("t2"."UPVOTE_COUNT" AS INTEGER) ELSE 0 END AS "FD_COL_7", CASE WHEN "t2"."DOWNVOTE_COUNT" IS NOT NULL THEN CAST("t2"."DOWNVOTE_COUNT" AS INTEGER) ELSE 0 END AS "FD_COL_8"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId" AND CAST("Posts"."PostTypeId" AS INTEGER) = 1
LEFT JOIN "STACK"."Posts" AS "Posts0" ON "Posts"."Id" = "Posts0"."ParentId"
LEFT JOIN (SELECT "PostId" AS "POSTID", COUNT(*) AS "COMMENT_COUNT"
FROM "STACK"."Comments"
GROUP BY "PostId") AS "t0" ON "Posts"."Id" = "t0"."POSTID"
LEFT JOIN (SELECT "Votes"."PostId" AS "POSTID", SUM(CASE WHEN "VoteTypes"."Name" = 'UpMod' THEN 1 ELSE 0 END) AS "UPVOTE_COUNT", SUM(CASE WHEN "VoteTypes"."Name" = 'DownMod' THEN 1 ELSE 0 END) AS "DOWNVOTE_COUNT"
FROM "STACK"."Votes"
INNER JOIN "STACK"."VoteTypes" ON "Votes"."VoteTypeId" = "VoteTypes"."Id"
GROUP BY "Votes"."PostId") AS "t2" ON "Posts"."Id" = "t2"."POSTID"
WHERE "Users"."Reputation" > 1000