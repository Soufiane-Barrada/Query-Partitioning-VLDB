SELECT COALESCE("company_type"."id", "company_type"."id") AS "id", "company_type"."kind", "movie_companies"."id" AS "id0", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "complete_cast"."id" AS "id1", "complete_cast"."movie_id" AS "movie_id0", "complete_cast"."subject_id", "complete_cast"."status_id", "t"."id" AS "id00", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum"
FROM "IMDB"."company_type"
INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id"
INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id") ON "movie_companies"."movie_id" = "t"."id"