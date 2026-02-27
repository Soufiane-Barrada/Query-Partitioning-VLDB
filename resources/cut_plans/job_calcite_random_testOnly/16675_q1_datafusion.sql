SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum", "movie_companies"."id" AS "id0", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t"
INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."movie_id"