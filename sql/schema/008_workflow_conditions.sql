-- +goose Up
ALTER TABLE jobs ADD COLUMN condition JSONB;

-- +goose Down
ALTER TABLE jobs DROP COLUMN condition;
