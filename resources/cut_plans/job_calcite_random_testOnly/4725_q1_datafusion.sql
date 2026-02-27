SELECT COALESCE("t"."TITLE_ID", "t"."TITLE_ID") AS "TITLE_ID", "t"."TITLE", "t"."PRODUCTION_YEAR", "t"."TITLE_RANK", "cast_info"."id", "cast_info"."person_id", "cast_info"."movie_id", "cast_info"."person_role_id", "cast_info"."note", "cast_info"."nr_order", "cast_info"."role_id"
FROM "IMDB"."cast_info"
RIGHT JOIN (SELECT "id" AS "TITLE_ID", "title" AS "TITLE", "production_year" AS "PRODUCTION_YEAR", ROW_NUMBER() OVER (PARTITION BY "production_year" ORDER BY "title") AS "TITLE_RANK"
FROM "IMDB"."aka_title") AS "t" ON "cast_info"."movie_id" = "t"."TITLE_ID"