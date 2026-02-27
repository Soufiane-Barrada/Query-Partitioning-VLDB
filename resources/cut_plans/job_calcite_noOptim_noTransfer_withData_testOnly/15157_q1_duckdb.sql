SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "AKA_NAME", "title"."title" AS "MOVIE_TITLE", "person_info"."info" AS "PERSON_INFO", "role_type"."role" AS "PERSON_ROLE", "title"."production_year"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
INNER JOIN "IMDB"."role_type" ON "cast_info"."role_id" = "role_type"."id"
INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id"
WHERE "title"."production_year" >= 2000