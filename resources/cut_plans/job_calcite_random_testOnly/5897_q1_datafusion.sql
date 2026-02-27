SELECT COALESCE("movie_companies"."id", "movie_companies"."id") AS "id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "t"."id" AS "id0", "t"."kind", "movie_keyword"."id" AS "id1", "movie_keyword"."movie_id" AS "movie_id0", "movie_keyword"."keyword_id", "t0"."id" AS "id00", "t0"."title", "t0"."imdb_index", "t0"."kind_id", "t0"."production_year", "t0"."imdb_id", "t0"."phonetic_code", "t0"."episode_of_id", "t0"."season_nr", "t0"."episode_nr", "t0"."series_years", "t0"."md5sum", "keyword"."id" AS "id10", "keyword"."keyword", "keyword"."phonetic_code" AS "phonetic_code0", "company_name"."id" AS "id2", "company_name"."name", "company_name"."country_code", "company_name"."imdb_id" AS "imdb_id0", "company_name"."name_pcode_nf", "company_name"."name_pcode_sf", "company_name"."md5sum" AS "md5sum0"
FROM "IMDB"."movie_companies"
INNER JOIN (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t" ON "movie_companies"."company_type_id" = "t"."id"
INNER JOIN ("IMDB"."movie_keyword" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t0" ON "movie_keyword"."movie_id" = "t0"."id" INNER JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id") ON "movie_companies"."movie_id" = "t0"."id"
INNER JOIN "IMDB"."company_name" ON "movie_companies"."company_id" = "company_name"."id"