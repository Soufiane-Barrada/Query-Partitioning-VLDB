SELECT COALESCE("t1"."id", "t1"."id") AS "MOVIE_ID", "t1"."title" AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "aka_name"."name", ', ') AS "ALIASES", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", LISTAGG(DISTINCT "t0"."name", ', ') AS "COMPANIES", LISTAGG(DISTINCT "name"."name" || ' (' || "s1"."role" || ')', ', ') AS "CAST_INFO"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."name" ON "aka_name"."person_id" = "name"."imdb_id") ON "s1"."person_id" = "aka_name"."person_id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."company_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."complete_cast" ON "t1"."id" = "complete_cast"."movie_id") ON "movie_companies"."movie_id" = "t1"."id") ON "movie_keyword"."movie_id" = "t1"."id") ON "s1"."id0" = "complete_cast"."subject_id"
GROUP BY "t1"."id", "t1"."title", "t1"."production_year"
ORDER BY "t1"."production_year" DESC NULLS FIRST, "t1"."title"