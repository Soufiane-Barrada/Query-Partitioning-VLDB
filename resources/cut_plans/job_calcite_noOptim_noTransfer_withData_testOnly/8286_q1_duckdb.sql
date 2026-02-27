SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "ACTOR_NAME", "title"."title" AS "MOVIE_TITLE", "title"."production_year" AS "PRODUCTION_YEAR", "comp_cast_type"."kind" AS "CAST_TYPE", "keyword"."keyword" AS "MOVIE_KEYWORD", "cast_info"."note" AS "CHARACTER_NOTE"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
INNER JOIN "IMDB"."movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"
INNER JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
INNER JOIN "IMDB"."comp_cast_type" ON "cast_info"."role_id" = "comp_cast_type"."id"
WHERE "title"."production_year" > 2000 AND "keyword"."keyword" LIKE 'action%'