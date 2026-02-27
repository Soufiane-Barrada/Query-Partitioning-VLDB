SELECT COALESCE("comp_cast_type"."id", "comp_cast_type"."id") AS "id", "comp_cast_type"."kind", "cast_info"."id" AS "id0", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id"
FROM "IMDB"."comp_cast_type"
INNER JOIN "IMDB"."cast_info" ON "comp_cast_type"."id" = "cast_info"."person_role_id"