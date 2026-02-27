SELECT COALESCE("aka_title"."id", "aka_title"."id") AS "id", "aka_title"."title", "aka_title"."production_year" AS "PRODUCTION_YEAR", "cast_info"."person_id", "keyword"."keyword", "company_name"."name", ', ' AS "FD_COL_6"
FROM "IMDB"."aka_title"
INNER JOIN "IMDB"."complete_cast" ON "aka_title"."id" = "complete_cast"."movie_id"
INNER JOIN "IMDB"."cast_info" ON "complete_cast"."subject_id" = "cast_info"."id"
LEFT JOIN "IMDB"."movie_keyword" ON "aka_title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
LEFT JOIN "IMDB"."movie_companies" ON "aka_title"."id" = "movie_companies"."movie_id"
LEFT JOIN "IMDB"."company_name" ON "movie_companies"."company_id" = "company_name"."id"
WHERE "aka_title"."production_year" >= 2000 AND "aka_title"."production_year" <= 2023