SELECT COALESCE("PostId", "PostId") AS "PostId", AVG("Score") AS "AVG_COMMENT_SCORE"
FROM "STACK"."Comments"
GROUP BY "PostId"