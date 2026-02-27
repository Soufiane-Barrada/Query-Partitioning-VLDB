SELECT COALESCE("company_type"."id", "company_type"."id") AS "id", "company_type"."kind", "movie_companies"."id" AS "id0", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM "IMDB"."company_type"
INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id"