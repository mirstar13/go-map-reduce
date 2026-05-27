-- +goose Up
CREATE TABLE plugin_cache (
    source_hash   TEXT        PRIMARY KEY, -- MinIO ETag (MD5)
    binary_path   TEXT        NOT NULL,
    last_used_at  TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_plugin_cache_last_used ON plugin_cache (last_used_at);

-- +goose Down
DROP TABLE IF EXISTS plugin_cache;
