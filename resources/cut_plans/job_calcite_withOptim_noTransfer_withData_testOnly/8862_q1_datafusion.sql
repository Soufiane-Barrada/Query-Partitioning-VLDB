SELECT COALESCE("role_type"."id", "role_type"."id") AS "id", "role_type"."role", "cast_info"."id" AS "id0", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id"
FROM "IMDB"."role_type"
INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."role_id"