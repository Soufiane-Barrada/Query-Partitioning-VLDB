SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."CAST_ORDER", "t2"."COMPANY_TYPE", "t2"."MOVIE_KEYWORD"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "cast_info"."nr_order" AS "CAST_ORDER", "company_type"."kind" AS "COMPANY_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."company_type" INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id") ON "movie_companies"."movie_id" = "t0"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "s1"."movie_id" = "t0"."id"
ORDER BY "t0"."title", "cast_info"."nr_order") AS "t2"