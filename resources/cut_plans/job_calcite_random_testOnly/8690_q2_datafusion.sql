SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."ROLE_IDENTIFICATION", "t3"."COMPANY_NAME", "t3"."MOVIE_KEYWORD", "t3"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "cast_info"."role_id" AS "ROLE_IDENTIFICATION", "t1"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1" INNER JOIN ("s1" INNER JOIN "IMDB"."movie_companies" ON "s1"."id" = "movie_companies"."movie_id") ON "t1"."id" = "movie_companies"."company_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "s1"."movie_id" = "cast_info"."movie_id") ON "movie_keyword"."movie_id" = "s1"."id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t3"