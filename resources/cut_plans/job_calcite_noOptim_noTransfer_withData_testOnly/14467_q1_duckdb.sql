SELECT COALESCE("title"."title", "title"."title") AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "role_type"."role" AS "ROLE_TYPE", "comp_cast_type"."kind" AS "COMP_CAST_TYPE", "movie_info"."info" AS "MOVIE_INFO"
FROM "IMDB"."title"
INNER JOIN "IMDB"."movie_info" ON "title"."id" = "movie_info"."movie_id"
INNER JOIN "IMDB"."complete_cast" ON "title"."id" = "complete_cast"."movie_id"
INNER JOIN "IMDB"."cast_info" ON "complete_cast"."subject_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."aka_name" ON "cast_info"."person_id" = "aka_name"."person_id"
INNER JOIN "IMDB"."role_type" ON "cast_info"."role_id" = "role_type"."id"
INNER JOIN "IMDB"."comp_cast_type" ON "cast_info"."person_role_id" = "comp_cast_type"."id"
WHERE "title"."production_year" >= 2000