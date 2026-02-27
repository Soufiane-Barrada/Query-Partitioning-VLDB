SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."PERSON_INFO", "t2"."MOVIE_KEYWORD", "t2"."COMPANY_NAME", "t2"."ROLE_TYPE", "t2"."production_year"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "person_info"."info" AS "PERSON_INFO", "keyword"."keyword" AS "MOVIE_KEYWORD", "s1"."name" AS "COMPANY_NAME", "role_type"."role" AS "ROLE_TYPE", "t0"."production_year"
FROM "s1"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id" INNER JOIN ("IMDB"."role_type" INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."role_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "s1"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "t0"."title") AS "t2"