SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "aka_title"."title", "company_type"."kind", "keyword"."keyword", "person_info"."info", "cast_info"."person_id" AS "person_id0"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."aka_title" ON "cast_info"."movie_id" = "aka_title"."movie_id"
INNER JOIN "IMDB"."movie_companies" ON "aka_title"."id" = "movie_companies"."movie_id"
INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id"
INNER JOIN "IMDB"."keyword" ON "aka_title"."id" = "keyword"."id"
INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id"
WHERE "aka_title"."production_year" > 2000 AND "company_type"."kind" LIKE '%production%'