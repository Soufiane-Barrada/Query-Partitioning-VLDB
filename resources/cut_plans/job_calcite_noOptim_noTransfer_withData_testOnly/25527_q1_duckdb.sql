SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "cast_info"."movie_id", "title"."title", ', ' AS "FD_COL_3", CAST("movie_info"."info" AS DECIMAL(19, 0)) AS "FD_COL_4", "keyword"."keyword"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
LEFT JOIN "IMDB"."movie_info" ON "title"."id" = "movie_info"."movie_id" AND "movie_info"."info_type_id" = (((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'rating')))
LEFT JOIN "IMDB"."movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
WHERE "title"."production_year" >= 2000