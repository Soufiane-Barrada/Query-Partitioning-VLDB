SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."name", "t"."country_code", "t"."imdb_id", "t"."name_pcode_nf", "t"."name_pcode_sf", "t"."md5sum", "movie_companies"."id" AS "id0", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "complete_cast"."id" AS "id1", "complete_cast"."movie_id" AS "movie_id0", "complete_cast"."subject_id", "complete_cast"."status_id", "t0"."id" AS "id00", "t0"."title", "t0"."imdb_index", "t0"."kind_id", "t0"."production_year", "t0"."imdb_id" AS "imdb_id0", "t0"."phonetic_code", "t0"."episode_of_id", "t0"."season_nr", "t0"."episode_nr", "t0"."series_years", "t0"."md5sum" AS "md5sum0"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t"
INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."company_id"
INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id") ON "movie_companies"."movie_id" = "t0"."id"