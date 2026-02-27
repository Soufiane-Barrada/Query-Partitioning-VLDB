SELECT COALESCE("Id", "Id") AS "Id", "PostHistoryTypeId", "PostId", "RevisionGUID", "CreationDate", "UserId", "UserDisplayName", "Comment", "Text", "ContentLicense"
FROM "STACK"."PostHistory"
WHERE CAST("PostHistoryTypeId" AS INTEGER) IN (4, 5, 6)