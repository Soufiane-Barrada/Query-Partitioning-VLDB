SELECT COALESCE("t0"."POSTID", "t0"."POSTID") AS "POSTID", "t0"."TITLE", "t0"."SCORE", "t0"."OWNERDISPLAYNAME", "t0"."CREATIONDATE", COUNT("Comments"."Id") AS "COMMENTCOUNT", CASE WHEN COUNT("Comments"."Id") > 10 THEN 'High Engagement    ' WHEN COUNT("Comments"."Id") >= 1 AND COUNT("Comments"."Id") <= 10 THEN 'Moderate Engagement' ELSE 'No Engagement      ' END AS "ENGAGEMENTLEVEL"
FROM "STACK"."Comments"
RIGHT JOIN (SELECT "t"."Id" AS "POSTID", "t"."Title" AS "TITLE", "t"."Score" AS "SCORE", "Users"."DisplayName" AS "OWNERDISPLAYNAME", "t"."CreationDate" AS "CREATIONDATE", ROW_NUMBER() OVER (PARTITION BY "t"."PostTypeId" ORDER BY "t"."CreationDate" DESC NULLS FIRST) AS "RN"
FROM (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '1' YEAR) AND "Score" > 0) AS "t"
LEFT JOIN "STACK"."Users" ON "t"."OwnerUserId" = "Users"."Id") AS "t0" ON "Comments"."PostId" = "t0"."POSTID"
GROUP BY "t0"."POSTID", "t0"."TITLE", "t0"."SCORE", "t0"."OWNERDISPLAYNAME", "t0"."CREATIONDATE"
HAVING CASE WHEN COUNT("Comments"."Id") > 10 THEN 'High Engagement    ' WHEN COUNT("Comments"."Id") >= 1 AND COUNT("Comments"."Id") <= 10 THEN 'Moderate Engagement' ELSE 'No Engagement      ' END <> 'No Engagement      '