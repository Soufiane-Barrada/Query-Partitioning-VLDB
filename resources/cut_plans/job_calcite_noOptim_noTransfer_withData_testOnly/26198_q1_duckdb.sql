SELECT COALESCE("title"."title", "title"."title") AS "title", "aka_name"."name", "role_type"."role", "cast_info"."note" AS "note0", "title"."production_year" AS "PRODUCTION_YEAR", "keyword"."keyword", ',' AS "FD_COL_6", "movie_companies"."company_id"
FROM "IMDB"."title"
INNER JOIN "IMDB"."movie_info" ON "title"."id" = "movie_info"."movie_id" AND "movie_info"."info_type_id" = (((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'summary')))
INNER JOIN "IMDB"."complete_cast" ON "title"."id" = "complete_cast"."movie_id"
INNER JOIN "IMDB"."cast_info" ON "complete_cast"."subject_id" = "cast_info"."id"
INNER JOIN "IMDB"."aka_name" ON "cast_info"."person_id" = "aka_name"."person_id"
INNER JOIN "IMDB"."role_type" ON "cast_info"."role_id" = "role_type"."id"
LEFT JOIN "IMDB"."movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
LEFT JOIN "IMDB"."movie_companies" ON "title"."id" = "movie_companies"."movie_id"
WHERE "title"."production_year" >= 2000 AND "title"."production_year" <= 2023 AND "aka_name"."name" LIKE '%Smith%'