SELECT COALESCE("movie_companies"."movie_id", "movie_companies"."movie_id") AS "MOVIE_ID", "company_name"."name" AS "COMPANY_NAME", "company_type"."kind" AS "COMPANY_TYPE"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."company_name" INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id") ON "company_type"."id" = "movie_companies"."company_type_id"