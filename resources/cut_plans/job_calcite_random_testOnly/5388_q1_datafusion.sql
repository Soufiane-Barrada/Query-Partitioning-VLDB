SELECT COALESCE("movie_companies"."id", "movie_companies"."id") AS "id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "movie_keyword"."id" AS "id0", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id", "t"."id" AS "id00", "t"."title", "t"."imdb_index", "t"."kind_id", "t"."production_year", "t"."imdb_id", "t"."phonetic_code", "t"."episode_of_id", "t"."season_nr", "t"."episode_nr", "t"."series_years", "t"."md5sum", "t0"."id" AS "id1", "t0"."keyword", "t0"."phonetic_code" AS "phonetic_code0", "company_type"."id" AS "id2", "company_type"."kind"
FROM "IMDB"."movie_companies"
INNER JOIN ("IMDB"."movie_keyword" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t" ON "movie_keyword"."movie_id" = "t"."id" INNER JOIN (SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" ILIKE '%thriller%') AS "t0" ON "movie_keyword"."keyword_id" = "t0"."id") ON "movie_companies"."movie_id" = "t"."id"
INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id"