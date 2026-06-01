-- +goose Up
ALTER TABLE jobs ADD COLUMN input_bucket VARCHAR(63) NOT NULL DEFAULT 'input';
ALTER TABLE jobs ADD COLUMN output_bucket VARCHAR(63) NOT NULL DEFAULT 'output';

-- +goose Down
ALTER TABLE jobs DROP COLUMN input_bucket;
ALTER TABLE jobs DROP COLUMN output_bucket;
