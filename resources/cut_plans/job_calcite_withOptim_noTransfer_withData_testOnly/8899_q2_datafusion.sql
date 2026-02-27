SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."PRODUCTION_YEAR", "t3"."MOVIE_KEYWORD", "t3"."COMPANY_TYPE"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."production_year" AS "PRODUCTION_YEAR", "s1"."keyword" AS "MOVIE_KEYWORD", "t1"."kind" AS "COMPANY_TYPE"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%Production%') AS "t1"
INNER JOIN "s1" ON "t1"."id" = "s1"."company_type_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."name"
FETCH NEXT 100 ROWS ONLY) AS "t3"