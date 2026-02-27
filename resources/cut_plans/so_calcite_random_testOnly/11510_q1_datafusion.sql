SELECT COALESCE(ANY_VALUE("VoteTypes"."Name"), ANY_VALUE("VoteTypes"."Name")) AS "VOTETYPENAME", COUNT(*) AS "VOTECOUNT"
FROM "STACK"."VoteTypes"
INNER JOIN "STACK"."Votes" ON "VoteTypes"."Id" = "Votes"."VoteTypeId"
GROUP BY "VoteTypes"."Name"