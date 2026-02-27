SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."PERSON_INFO", "t2"."CAST_TYPE", "t2"."MOVIE_KEYWORD", "t2"."production_year"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "person_info"."info" AS "PERSON_INFO", "s1"."kind" AS "CAST_TYPE", "keyword"."keyword" AS "MOVIE_KEYWORD", "t0"."production_year"
FROM "IMDB"."movie_info"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id" INNER JOIN ("s1" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id") ON "s1"."id0" = "complete_cast"."subject_id") ON "aka_name"."person_id" = "s1"."person_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "movie_info"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t2"