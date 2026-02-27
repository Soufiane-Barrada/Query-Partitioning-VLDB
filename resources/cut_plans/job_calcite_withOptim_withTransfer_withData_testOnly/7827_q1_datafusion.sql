SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."kind", "role_type"."id" AS "id0", "role_type"."role", "cast_info"."id" AS "id00", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id"
FROM (SELECT *
FROM "IMDB"."comp_cast_type"
WHERE "kind" = 'Cast') AS "t"
INNER JOIN ("IMDB"."role_type" INNER JOIN "IMDB"."cast_info" ON "role_type"."id" = "cast_info"."role_id") ON "t"."id" = "cast_info"."person_role_id"