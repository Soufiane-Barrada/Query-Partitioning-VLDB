SELECT COALESCE(ANY_VALUE("Id"), ANY_VALUE("Id")) AS "POSTID", "Title" AS "TITLE", "CreationDate" AS "CREATIONDATE", "Score" AS "SCORE", "ViewCount" AS "VIEWCOUNT", ANY_VALUE("DisplayName") AS "OWNERDISPLAYNAME", "Reputation" AS "REPUTATION", COUNT("Id1") AS "COMMENTCOUNT", COUNT("Id2") AS "VOTECOUNT", "TagName" AS "TAGNAME", "PostHistoryTypeId" AS "POSTHISTORYTYPEID", ANY_VALUE("CreationDate3") AS "HISTORYCREATIONDATE"
FROM "s1"
WHERE "CreationDate" >= CAST(DATE '2023-01-01' AS TIMESTAMP(0)) AND "CreationDate" < CAST(DATE '2024-01-01' AS TIMESTAMP(0))
GROUP BY "Id", "Title", "CreationDate", "Score", "ViewCount", "DisplayName", "Reputation", "TagName", "PostHistoryTypeId", "CreationDate3"
ORDER BY "Score" DESC NULLS FIRST, "ViewCount" DESC NULLS FIRST