SELECT COALESCE("movie_companies"."id", "movie_companies"."id") AS "id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "t"."id" AS "id0", "t"."movie_id" AS "movie_id0", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note" AS "note0", "t"."md5sum"
FROM "IMDB"."movie_companies"
INNER JOIN (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t" ON "movie_companies"."movie_id" = "t"."id"