SELECT COALESCE("company_type"."id", "company_type"."id") AS "id", "company_type"."kind", "t"."id" AS "id0", "t"."movie_id", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."note", "t"."md5sum", "movie_companies"."id" AS "id00", "movie_companies"."movie_id" AS "movie_id0", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note" AS "note0"
FROM "IMDB"."company_type"
INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" = 2020) AS "t" INNER JOIN "IMDB"."movie_companies" ON "t"."movie_id" = "movie_companies"."movie_id") ON "company_type"."id" = "movie_companies"."company_type_id"