SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "ACTOR_NAME", "aka_name"."imdb_index" AS "ACTOR_IMDB_INDEX", "aka_title"."title" AS "MOVIE_TITLE", "aka_title"."production_year" AS "MOVIE_YEAR", "cast_info"."role_id" AS "ROLE_ID", "company_type"."kind" AS "COMPANY_TYPE", "company_name"."name" AS "COMPANY_NAME", "movie_info"."info" AS "MOVIE_INFO", "keyword"."keyword" AS "MOVIE_KEYWORD"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."aka_title" ON "cast_info"."movie_id" = "aka_title"."movie_id"
INNER JOIN "IMDB"."movie_companies" ON "aka_title"."id" = "movie_companies"."movie_id"
INNER JOIN "IMDB"."company_name" ON "movie_companies"."company_id" = "company_name"."id"
INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id"
LEFT JOIN "IMDB"."movie_info" ON "aka_title"."id" = "movie_info"."movie_id" AND "movie_info"."info_type_id" = (((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Box Office')))
LEFT JOIN "IMDB"."movie_keyword" ON "aka_title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
WHERE "aka_title"."production_year" > 2000