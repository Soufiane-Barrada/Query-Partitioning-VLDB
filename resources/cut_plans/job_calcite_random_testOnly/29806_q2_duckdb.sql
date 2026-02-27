SELECT COALESCE("s1"."id", "s1"."id") AS "MOVIE_ID", "s1"."title" AS "MOVIE_TITLE", "s1"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "t1"."keyword", ', ') AS "KEYWORDS", LISTAGG(DISTINCT "company_name"."name", ', ') AS "COMPANIES", LISTAGG(DISTINCT "name"."name" || ' (' || "role_type"."role" || ')', ', ') AS "CAST_DETAILS"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN ("IMDB"."role_type" INNER JOIN ("IMDB"."name" INNER JOIN "IMDB"."cast_info" ON "name"."id" = "cast_info"."person_id") ON "role_type"."id" = "cast_info"."role_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN "s1" ON "complete_cast"."movie_id" = "s1"."id" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%Action%') AS "t1" INNER JOIN "IMDB"."movie_keyword" ON "t1"."id" = "movie_keyword"."keyword_id") ON "s1"."id" = "movie_keyword"."movie_id") ON "cast_info"."person_id" = "complete_cast"."subject_id") ON "movie_companies"."movie_id" = "s1"."id"
GROUP BY "s1"."id", "s1"."title", "s1"."production_year"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."title"