SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."PostTypeId", "t"."AcceptedAnswerId", "t"."ParentId", "t"."CreationDate", "t"."Score", "t"."ViewCount", "t"."Body", "t"."OwnerUserId", "t"."OwnerDisplayName", "t"."LastEditorUserId", "t"."LastEditorDisplayName", "t"."LastEditDate", "t"."LastActivityDate", "t"."Title", "t"."Tags", "t"."AnswerCount", "t"."CommentCount", "t"."FavoriteCount", "t"."ClosedDate", "t"."CommunityOwnedDate", "t"."ContentLicense", "Users"."Id" AS "Id0", "Users"."Reputation", "Users"."CreationDate" AS "CreationDate0", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", CASE WHEN "t4"."EXPR$0" IS NULL THEN 0 ELSE "t4"."EXPR$0" END AS "FD_COL_35"
FROM (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t"
INNER JOIN "STACK"."Users" ON "t"."OwnerUserId" = "Users"."Id"
LEFT JOIN (SELECT "t1"."Id" AS "PostId", CASE WHEN "t3"."EXPR$0" IS NOT NULL THEN "t3"."EXPR$0" ELSE 0 END AS "EXPR$0"
FROM (SELECT "t0"."Id"
FROM (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t0"
INNER JOIN "STACK"."Users" AS "Users0" ON "t0"."OwnerUserId" = "Users0"."Id") AS "t1"
LEFT JOIN (SELECT "PostId", COUNT(*) AS "EXPR$0"
FROM "STACK"."Comments"
GROUP BY "PostId") AS "t3" ON "t1"."Id" IS NOT DISTINCT FROM "t3"."PostId") AS "t4" ON "t"."Id" = "t4"."PostId"