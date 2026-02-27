SELECT COALESCE("VoteTypeId", "VoteTypeId") AS "VoteTypeId", COUNT(*) AS "TOTALVOTES"
FROM "STACK"."Votes"
GROUP BY "VoteTypeId"