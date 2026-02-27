SELECT COALESCE("t4"."ACTOR_NAME", "t4"."ACTOR_NAME") AS "ACTOR_NAME", "t4"."MOVIE_TITLE", "t4"."COMPANY_TYPE", "t4"."MOVIE_KEYWORD", "t4"."ACTOR_INFO"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "t2"."kind" AS "COMPANY_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD", "s1"."info" AS "ACTOR_INFO"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t2"
INNER JOIN "s1" ON "t2"."id" = "s1"."company_type_id"
ORDER BY "s1"."name", "s1"."title"
FETCH NEXT 100 ROWS ONLY) AS "t4"