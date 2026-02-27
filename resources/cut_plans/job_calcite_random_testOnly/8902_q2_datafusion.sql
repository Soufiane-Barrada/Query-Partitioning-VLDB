SELECT COALESCE("t2"."ACTOR_NAME", "t2"."ACTOR_NAME") AS "ACTOR_NAME", "t2"."MOVIE_TITLE", "t2"."ROLE_ORDER", "t2"."COMPANY_TYPE", "t2"."MOVIE_KEYWORD", "t2"."MOVIE_INFO", "t2"."ACTOR_GENDER", "t2"."production_year"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "t0"."title" AS "MOVIE_TITLE", "cast_info"."nr_order" AS "ROLE_ORDER", "company_type"."kind" AS "COMPANY_TYPE", "keyword"."keyword" AS "MOVIE_KEYWORD", "movie_info"."info" AS "MOVIE_INFO", "s1"."gender" AS "ACTOR_GENDER", "t0"."production_year"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."movie_info" INNER JOIN ("s1" INNER JOIN ("IMDB"."cast_info" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "cast_info"."movie_id" = "t0"."movie_id") ON "s1"."person_id" = "cast_info"."person_id") ON "movie_info"."movie_id" = "t0"."id") ON "movie_keyword"."movie_id" = "t0"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "s1"."name", "cast_info"."nr_order"
FETCH NEXT 100 ROWS ONLY) AS "t2"