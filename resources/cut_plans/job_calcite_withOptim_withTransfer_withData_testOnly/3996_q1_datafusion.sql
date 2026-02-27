SELECT COALESCE("cast_info"."movie_id", "cast_info"."movie_id") AS "MOVIE_ID", COUNT(DISTINCT "cast_info"."person_id") AS "ACTOR_COUNT", LISTAGG(DISTINCT "role_type"."role", ', ') AS "ROLES"
FROM "IMDB"."role_type"
INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."person_role_id"
GROUP BY "cast_info"."movie_id"