SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "ACTOR_NAME", "title"."title" AS "MOVIE_TITLE", "title"."production_year" AS "PRODUCTION_YEAR", "keyword"."keyword" AS "MOVIE_KEYWORD", "company_type"."kind" AS "COMPANY_TYPE"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
INNER JOIN "IMDB"."movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"
INNER JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
INNER JOIN "IMDB"."movie_companies" ON "title"."id" = "movie_companies"."movie_id"
INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id"
WHERE "title"."production_year" >= 2000 AND "title"."production_year" <= 2023 AND "company_type"."kind" LIKE '%Production%'