SELECT COALESCE("movie_companies"."movie_id", "movie_companies"."movie_id") AS "MOVIE_ID", LISTAGG(DISTINCT "company_name"."name", ', ') AS "COMPANY_NAMES", LISTAGG(DISTINCT "company_type"."kind", ', ') AS "COMPANY_TYPES"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."company_name" INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id") ON "company_type"."id" = "movie_companies"."company_type_id"
GROUP BY "movie_companies"."movie_id"