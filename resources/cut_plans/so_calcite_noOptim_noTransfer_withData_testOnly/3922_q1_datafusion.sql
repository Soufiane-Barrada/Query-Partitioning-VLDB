SELECT COALESCE("OwnerUserId", "OwnerUserId") AS "OWNERUSERID", "ViewCount", "Score"
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR)