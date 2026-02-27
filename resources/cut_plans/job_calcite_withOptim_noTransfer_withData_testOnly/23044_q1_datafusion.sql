SELECT COALESCE("cast_info"."movie_id", "cast_info"."movie_id") AS "MOVIE_ID", "cast_info"."person_id", CASE WHEN "role_type"."role" = 'Director' THEN 1 ELSE 0 END AS "FD_COL_2"
FROM "IMDB"."role_type"
INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."role_id"