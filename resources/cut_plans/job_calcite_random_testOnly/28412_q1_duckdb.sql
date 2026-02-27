SELECT COALESCE("id", "id") AS "id", "title", "imdb_index", "kind_id", "production_year", "imdb_id", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "series_years", "md5sum"
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023