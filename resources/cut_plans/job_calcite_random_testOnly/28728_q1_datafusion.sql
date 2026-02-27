SELECT COALESCE("t0"."MOVIE_ID", "t0"."MOVIE_ID") AS "MOVIE_ID", "t0"."ACTOR_NAME", "t0"."ROLE_NAME", "t0"."CAST_TYPE", "t2"."MOVIE_ID" AS "MOVIE_ID0", "t2"."COMPANY_NAME", "t2"."COMPANY_TYPE", "t5"."MOVIE_ID" AS "MOVIE_ID00", "t5"."MOVIE_TITLE", "t5"."PRODUCTION_YEAR", "t5"."MOVIE_KEYWORD", "t5"."TITLE_KIND"
FROM (SELECT "cast_info"."movie_id" AS "MOVIE_ID", "aka_name"."name" AS "ACTOR_NAME", "t"."role" AS "ROLE_NAME", "comp_cast_type"."kind" AS "CAST_TYPE"
FROM (SELECT *
FROM "IMDB"."role_type"
WHERE "role" LIKE '%Actor%') AS "t"
INNER JOIN ("IMDB"."comp_cast_type" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "comp_cast_type"."id" = "cast_info"."person_role_id") ON "t"."id" = "cast_info"."role_id") AS "t0"
INNER JOIN ((SELECT "movie_companies"."movie_id" AS "MOVIE_ID", "company_name"."name" AS "COMPANY_NAME", "t1"."kind" AS "COMPANY_TYPE"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%Production%') AS "t1"
INNER JOIN ("IMDB"."company_name" INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id") ON "t1"."id" = "movie_companies"."company_type_id") AS "t2" INNER JOIN (SELECT "t4"."id" AS "MOVIE_ID", "t4"."title" AS "MOVIE_TITLE", "t4"."production_year" AS "PRODUCTION_YEAR", "keyword"."keyword" AS "MOVIE_KEYWORD", "t4"."kind" AS "TITLE_KIND"
FROM (SELECT "t3"."id", "t3"."title", "t3"."imdb_index", "t3"."kind_id", "t3"."production_year", "t3"."imdb_id", "t3"."phonetic_code", "t3"."episode_of_id", "t3"."season_nr", "t3"."episode_nr", "t3"."series_years", "t3"."md5sum", "kind_type"."id" AS "id0", "kind_type"."kind", "movie_keyword"."id" AS "id1", "movie_keyword"."movie_id", "movie_keyword"."keyword_id"
FROM "IMDB"."movie_keyword"
RIGHT JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2020) AS "t3" LEFT JOIN "IMDB"."kind_type" ON "t3"."kind_id" = "kind_type"."id") ON "movie_keyword"."movie_id" = "t3"."id") AS "t4"
LEFT JOIN "IMDB"."keyword" ON "t4"."keyword_id" = "keyword"."id") AS "t5" ON "t2"."MOVIE_ID" = "t5"."MOVIE_ID") ON "t0"."MOVIE_ID" = "t5"."MOVIE_ID"