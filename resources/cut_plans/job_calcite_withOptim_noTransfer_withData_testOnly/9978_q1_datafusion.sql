SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."info", "movie_info"."id" AS "id0", "movie_info"."movie_id", "movie_info"."info_type_id", "movie_info"."info" AS "info0", "movie_info"."note"
FROM (SELECT *
FROM "IMDB"."info_type"
WHERE "info" = 'budget') AS "t"
INNER JOIN "IMDB"."movie_info" ON "t"."id" = "movie_info"."info_type_id"