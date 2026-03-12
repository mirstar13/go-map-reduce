-- +goose Up
CREATE TABLE users (
    user_id         UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    username        VARCHAR(64)     UNIQUE NOT NULL,
    password_hash   VARCHAR(255)    NOT NULL,
    role            VARCHAR(16)     NOT NULL DEFAULT 'user',
    email           VARCHAR(255),
    create_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE users;