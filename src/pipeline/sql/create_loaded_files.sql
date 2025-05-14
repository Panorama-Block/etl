CREATE TABLE IF NOT EXISTS loaded_files
(
    file_name String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY file_name;