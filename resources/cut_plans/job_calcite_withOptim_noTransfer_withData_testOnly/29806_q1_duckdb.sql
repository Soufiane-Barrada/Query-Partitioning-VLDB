SELECT COALESCE("t"."id", "t"."id") AS "MOVIE_ID", "t"."title" AS "MOVIE_TITLE", "t"."production_year" AS "PRODUCTION_YEAR", "t0"."keyword" AS "MOVIE_KEYWORD", ', ' AS "FD_COL_4", "company_name"."name" AS "COMPANY_NAME", "name"."name" || ' (' || "role_type"."role" || ')' AS "FD_COL_6"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN ("IMDB"."role_type" INNER JOIN ("IMDB"."name" INNER JOIN "IMDB"."cast_info" ON "name"."id" = "cast_info"."person_id") ON "role_type"."id" = "cast_info"."role_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2010) AS "t" ON "complete_cast"."movie_id" = "t"."id" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%Action%') AS "t0" INNER JOIN "IMDB"."movie_keyword" ON "t0"."id" = "movie_keyword"."keyword_id") ON "t"."id" = "movie_keyword"."movie_id") ON "cast_info"."person_id" = "complete_cast"."subject_id") ON "movie_companies"."movie_id" = "t"."id"