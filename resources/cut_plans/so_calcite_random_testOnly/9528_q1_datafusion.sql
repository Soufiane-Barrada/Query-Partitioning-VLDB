SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."Reputation", "t"."CreationDate", "t"."DisplayName", "t"."LastAccessDate", "t"."WebsiteUrl", "t"."Location", "t"."AboutMe", "t"."Views", "t"."UpVotes", "t"."DownVotes", "t"."ProfileImageUrl", "t"."AccountId", "t1"."USERID", "t1"."TOTALVOTES", "t1"."UPVOTES" AS "UPVOTES_", "t1"."DOWNVOTES" AS "DOWNVOTES_"
FROM (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t"
LEFT JOIN (SELECT "UserId" AS "USERID", COUNT(*) AS "TOTALVOTES", SUM(CASE WHEN CAST("VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES"
FROM "STACK"."Votes"
GROUP BY "UserId") AS "t1" ON "t"."Id" = "t1"."USERID"