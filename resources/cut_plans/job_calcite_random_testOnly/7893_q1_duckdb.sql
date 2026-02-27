SELECT COALESCE("info_type"."id", "info_type"."id") AS "id", "info_type"."info", "movie_info"."id" AS "id0", "movie_info"."movie_id", "movie_info"."info_type_id", "movie_info"."info" AS "info0", "movie_info"."note"
FROM "IMDB"."info_type"
INNER JOIN "IMDB"."movie_info" ON "info_type"."id" = "movie_info"."info_type_id"