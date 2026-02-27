SELECT COALESCE("name", "name") AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "nr_order" AS "CAST_ORDER", "info" AS "MOVIE_INFO"
FROM "s1"
WHERE "info_type_id" = (((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'box office')))
ORDER BY "nr_order"