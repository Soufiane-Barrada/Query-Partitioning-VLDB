SELECT COALESCE("company_type"."id", "company_type"."id") AS "id", "company_type"."kind", "t"."id" AS "id0", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum", "movie_companies"."id" AS "id00", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM "IMDB"."company_type"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2022) AS "t" INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."movie_id") ON "company_type"."id" = "movie_companies"."company_type_id"