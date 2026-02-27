SELECT COALESCE("title"."title", "title"."title") AS "MOVIE_TITLE", "title"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "company_name"."name", ', ') AS "COMPANIES", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", LISTAGG(DISTINCT "s1"."name" || ' (' || "role_type"."role" || ')', ', ') AS "CAST"
FROM "IMDB"."company_name"
INNER JOIN ("IMDB"."role_type" INNER JOIN "s1" ON "role_type"."id" = "s1"."role_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."title" INNER JOIN "IMDB"."movie_companies" ON "title"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "title"."id") ON "s1"."movie_id" = "title"."id") ON "company_name"."id" = "movie_companies"."company_id"
GROUP BY "title"."title", "title"."production_year"
ORDER BY "title"."production_year" DESC NULLS FIRST, "title"."title"
FETCH NEXT 100 ROWS ONLY