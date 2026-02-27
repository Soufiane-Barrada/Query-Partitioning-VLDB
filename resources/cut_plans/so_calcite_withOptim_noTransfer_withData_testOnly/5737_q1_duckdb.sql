SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."DisplayName" AS "DISPLAYNAME", "t"."Reputation" AS "REPUTATION", "t"."Views" AS "VIEWS", "t"."UpVotes" AS "UPVOTES", "t"."DownVotes" AS "DOWNVOTES", "Posts"."Id" AS "Id0", CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END AS "FD_COL_7", CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_8", CASE WHEN "Posts"."ClosedDate" IS NOT NULL THEN 1 ELSE 0 END AS "FD_COL_9"
FROM "STACK"."Posts"
RIGHT JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t" ON "Posts"."OwnerUserId" = "t"."Id"