SELECT COALESCE(ANY_VALUE("s1"."Id"), ANY_VALUE("s1"."Id")) AS "POSTID", "s1"."Title" AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "s1"."Score" AS "SCORE", "s1"."ViewCount" AS "VIEWCOUNT", ANY_VALUE("s1"."DisplayName") AS "OWNERDISPLAYNAME", ANY_VALUE("s1"."Reputation") AS "OWNERREPUTATION", ARRAY_AGG("s1"."TagName") AS "TAGS", COUNT("Votes"."Id") AS "VOTECOUNT"
FROM "s1"
LEFT JOIN "STACK"."Votes" ON "s1"."Id" = "Votes"."PostId"
WHERE "s1"."CreationDate" >= '2023-01-01'
GROUP BY "s1"."Id", "s1"."Title", "s1"."CreationDate", "s1"."Score", "s1"."ViewCount", "s1"."DisplayName", "s1"."Reputation"
ORDER BY "s1"."CreationDate" DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY