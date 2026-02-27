SELECT COALESCE("PostId", "PostId") AS "PostId", COUNT(*) AS "EDITCOUNT", MAX("CreationDate") AS "MOSTRECENTEDIT"
FROM "STACK"."PostHistory"
GROUP BY "PostId"