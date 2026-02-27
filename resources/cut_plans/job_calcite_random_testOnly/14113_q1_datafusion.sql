SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."movie_id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note", "t"."md5sum", "movie_companies"."id" AS "id0", "movie_companies"."movie_id" AS "movie_id0", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note" AS "note0"
FROM (SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t"
INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."movie_id"