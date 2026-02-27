SELECT COALESCE("t4"."MOVIE_TITLE", "t4"."MOVIE_TITLE") AS "MOVIE_TITLE", "t4"."PRODUCTION_YEAR", "t4"."ACTOR_COUNT", "t4"."KEYWORDS", "t4"."PRODUCTION_COMPANIES"
FROM (SELECT "t1"."title" AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", COUNT(DISTINCT "aka_name"."name") AS "ACTOR_COUNT", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", LISTAGG(DISTINCT "s1"."name", ', ') AS "PRODUCTION_COMPANIES"
FROM "IMDB"."cast_info"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."company_type" INNER JOIN "s1" ON "company_type"."id" = "s1"."company_type_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1" ON "complete_cast"."movie_id" = "t1"."id") ON "s1"."movie_id" = "t1"."id") ON "movie_keyword"."movie_id" = "t1"."id") ON "cast_info"."id" = "complete_cast"."subject_id"
GROUP BY "t1"."title", "t1"."production_year"
ORDER BY "t1"."production_year" DESC NULLS FIRST, 3 DESC NULLS FIRST) AS "t4"