SELECT COALESCE("aka_name"."person_id", "aka_name"."person_id") AS "PERSON_ID", "aka_name"."name" AS "NAME", "cast_info"."movie_id", "aka_title"."title", ', ' AS "FD_COL_4"
FROM "IMDB"."aka_title"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "aka_title"."id" = "cast_info"."movie_id"