SELECT COALESCE("PostLinks"."PostId", "PostLinks"."PostId") AS "POSTID", COUNT(DISTINCT "PostLinks"."RelatedPostId") AS "RELATEDPOSTCOUNT", SUM(CASE WHEN "LinkTypes"."Name" = 'Duplicate' THEN 1 ELSE 0 END) AS "DUPLICATECOUNT"
FROM "STACK"."LinkTypes"
INNER JOIN "STACK"."PostLinks" ON "LinkTypes"."Id" = "PostLinks"."LinkTypeId"
GROUP BY "PostLinks"."PostId"