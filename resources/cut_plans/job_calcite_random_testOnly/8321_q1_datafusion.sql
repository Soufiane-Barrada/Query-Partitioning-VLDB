SELECT COALESCE("movie_companies"."movie_id", "movie_companies"."movie_id") AS "movie_id", "company_name"."name", "company_type"."kind", ANY_VALUE("company_name"."name") AS "COMPANY_NAME", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_MOVIES"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."company_name" INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id") ON "company_type"."id" = "movie_companies"."company_type_id"
GROUP BY "company_type"."kind", "company_name"."name", "movie_companies"."movie_id"