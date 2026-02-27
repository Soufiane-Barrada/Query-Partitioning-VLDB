SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "title"."title", "title"."production_year" AS "PRODUCTION_YEAR", "comp_cast_type"."kind", "keyword"."keyword", ', ' AS "FD_COL_5"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
INNER JOIN "IMDB"."comp_cast_type" ON "cast_info"."person_role_id" = "comp_cast_type"."id"
LEFT JOIN "IMDB"."movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
WHERE "title"."production_year" >= 2000 AND "title"."production_year" <= 2023