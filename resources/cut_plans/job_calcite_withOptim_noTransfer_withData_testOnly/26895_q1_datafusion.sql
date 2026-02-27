SELECT COALESCE("cast_info"."movie_id", "cast_info"."movie_id") AS "MOVIE_ID", "role_type"."role", ', ' AS "FD_COL_2"
FROM "IMDB"."role_type"
INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."role_id"