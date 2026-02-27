SELECT COALESCE("title"."id", "title"."id") AS "id", "title"."title" AS "TITLE", "title"."production_year" AS "PRODUCTION_YEAR", "cast_info"."person_id", "aka_name"."name", ', ' AS "FD_COL_5"
FROM "IMDB"."title"
INNER JOIN "IMDB"."movie_companies" ON "title"."id" = "movie_companies"."movie_id"
INNER JOIN "IMDB"."cast_info" ON "movie_companies"."movie_id" = "cast_info"."movie_id"
INNER JOIN "IMDB"."aka_name" ON "cast_info"."person_id" = "aka_name"."person_id"
WHERE "title"."production_year" >= 2000