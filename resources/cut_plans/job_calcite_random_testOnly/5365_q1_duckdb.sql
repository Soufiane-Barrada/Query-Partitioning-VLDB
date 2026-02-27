SELECT COALESCE("keyword"."id", "keyword"."id") AS "id", "keyword"."keyword", "keyword"."phonetic_code", "movie_keyword"."id" AS "id0", "movie_keyword"."movie_id", "movie_keyword"."keyword_id", "t"."id" AS "id1", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code" AS "phonetic_code0", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum", "movie_companies"."id" AS "id00", "movie_companies"."movie_id" AS "movie_id0", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM "IMDB"."keyword"
INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t"."id"