SELECT COALESCE("aka_name"."id", "aka_name"."id") AS "AKA_ID", "aka_name"."name" AS "AKA_NAME", "title"."id" AS "TITLE_ID", "title"."title" AS "MOVIE_TITLE", "title"."production_year" AS "PRODUCTION_YEAR", "person_info"."info" AS "PERSON_INFO", "comp_cast_type"."kind" AS "CAST_KIND"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id"
INNER JOIN "IMDB"."role_type" ON "cast_info"."role_id" = "role_type"."id"
INNER JOIN "IMDB"."comp_cast_type" ON "cast_info"."person_role_id" = "comp_cast_type"."id"
WHERE "title"."production_year" >= 2000 AND "comp_cast_type"."kind" = 'actor'