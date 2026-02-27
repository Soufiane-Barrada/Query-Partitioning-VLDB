SELECT COALESCE("id", "id") AS "id", "person_id", "movie_id", "person_role_id", "note", "nr_order", "role_id"
FROM "IMDB"."cast_info"
WHERE "nr_order" < 10