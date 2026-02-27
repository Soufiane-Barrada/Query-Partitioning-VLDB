SELECT COALESCE("movie_companies"."id", "movie_companies"."id") AS "id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "t"."id" AS "id0", "t"."kind"
FROM "IMDB"."movie_companies"
INNER JOIN (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%Film%') AS "t" ON "movie_companies"."company_type_id" = "t"."id"