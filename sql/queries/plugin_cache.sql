-- name: GetCachedPlugin :one
SELECT * FROM plugin_cache
WHERE source_hash = $1
LIMIT 1;

-- name: UpsertCachedPlugin :exec
INSERT INTO plugin_cache (source_hash, binary_path, last_used_at)
VALUES ($1, $2, NOW())
ON CONFLICT (source_hash) 
DO UPDATE SET 
    last_used_at = NOW(),
    binary_path = EXCLUDED.binary_path;

-- name: UpdatePluginLastUsed :exec
UPDATE plugin_cache
SET last_used_at = NOW()
WHERE source_hash = $1;

-- name: ListStalePlugins :many
SELECT * FROM plugin_cache
WHERE last_used_at < NOW() - ($1 || ' seconds')::INTERVAL;

-- name: DeleteCachedPlugin :exec
DELETE FROM plugin_cache
WHERE source_hash = $1;
