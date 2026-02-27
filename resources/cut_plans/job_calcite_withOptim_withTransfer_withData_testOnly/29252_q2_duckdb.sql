SELECT COALESCE("t2"."title", "t2"."title") AS "MOVIE_TITLE", "t2"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "aka_name"."name", ', ') AS "ACTORS", LISTAGG(DISTINCT "company_type"."kind", ', ') AS "COMPANIES", LISTAGG(DISTINCT "s1"."info", '; ') AS "INFORMATIONS", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t2" INNER JOIN "IMDB"."movie_companies" ON "t2"."id" = "movie_companies"."movie_id") ON "s1"."movie_id" = "t2"."id") ON "cast_info"."movie_id" = "t2"."id") ON "movie_keyword"."movie_id" = "t2"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
GROUP BY "t2"."title", "t2"."production_year"
ORDER BY "t2"."production_year" DESC NULLS FIRST, "t2"."title"