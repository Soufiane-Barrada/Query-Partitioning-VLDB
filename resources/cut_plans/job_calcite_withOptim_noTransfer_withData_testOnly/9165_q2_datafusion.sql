SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_TYPE", "t3"."MOVIE_KEYWORD", "t3"."PERSON_INFO", "t3"."production_year"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "company_type"."kind" AS "COMPANY_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD", "s1"."info" AS "PERSON_INFO", "s1"."production_year"
FROM "IMDB"."company_type"
INNER JOIN "s1" ON "company_type"."id" = "s1"."company_type_id"
ORDER BY "s1"."name", "s1"."production_year" DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t3"