SELECT COALESCE("Votes"."PostId", "Votes"."PostId") AS "POSTID", CASE WHEN "VoteTypes"."Name" = 'UpMod' THEN 1 ELSE 0 END AS "FD_COL_1", CASE WHEN "VoteTypes"."Name" = 'DownMod' THEN 1 ELSE 0 END AS "FD_COL_2"
FROM "STACK"."VoteTypes"
INNER JOIN "STACK"."Votes" ON "VoteTypes"."Id" = "Votes"."VoteTypeId"