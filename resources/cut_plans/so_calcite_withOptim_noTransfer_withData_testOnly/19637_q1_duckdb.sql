SELECT COALESCE("Tags"."Id", "Tags"."Id") AS "Id", "Tags"."TagName", "Tags"."Count", "Tags"."ExcerptPostId", "Tags"."WikiPostId", "Tags"."IsModeratorOnly", "Tags"."IsRequired", "t"."Id" AS "Id0", "t"."PostTypeId", "t"."AcceptedAnswerId", "t"."ParentId", "t"."CreationDate", "t"."Score", "t"."ViewCount", "t"."Body", "t"."OwnerUserId", "t"."OwnerDisplayName", "t"."LastEditorUserId", "t"."LastEditorDisplayName", "t"."LastEditDate", "t"."LastActivityDate", "t"."Title", "t"."Tags", "t"."AnswerCount", "t"."CommentCount", "t"."FavoriteCount", "t"."ClosedDate", "t"."CommunityOwnedDate", "t"."ContentLicense"
FROM "STACK"."Tags"
INNER JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t" ON "t"."Tags" LIKE CONCAT('%', "Tags"."TagName", '%')