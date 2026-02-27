SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."person_id", "t"."movie_id", "t"."person_role_id", "t"."note", "t"."nr_order", "t"."role_id", "comp_cast_type"."id" AS "id0", "comp_cast_type"."kind", "t0"."id" AS "id1", "t0"."person_id" AS "person_id0", "t0"."name", "t0"."imdb_index", "t0"."name_pcode_cf", "t0"."name_pcode_nf", "t0"."surname_pcode", "t0"."md5sum", "t1"."id" AS "id2", "t1"."title", "t1"."imdb_index" AS "imdb_index0", "t1"."kind_id", "t1"."production_year", "t1"."imdb_id", "t1"."phonetic_code", "t1"."episode_of_id", "t1"."season_nr", "t1"."episode_nr", "t1"."series_years", "t1"."md5sum" AS "md5sum0"
FROM (SELECT *
FROM "IMDB"."cast_info"
WHERE "nr_order" <= 5) AS "t"
INNER JOIN "IMDB"."comp_cast_type" ON "t"."person_role_id" = "comp_cast_type"."id"
INNER JOIN (SELECT *
FROM "IMDB"."aka_name"
WHERE "name" LIKE '%Smith%') AS "t0" ON "t"."person_id" = "t0"."person_id"
INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t1" ON "t"."movie_id" = "t1"."id"