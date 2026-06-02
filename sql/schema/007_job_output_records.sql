-- +goose Up
ALTER TABLE jobs ADD COLUMN output_records BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE jobs DROP COLUMN output_records;
