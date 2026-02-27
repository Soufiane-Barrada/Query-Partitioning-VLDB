SELECT COALESCE("id", "id") AS "id", "movie_id", "title", "imdb_index", "kind_id", "production_year", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "note", "md5sum"
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023