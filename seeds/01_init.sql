-- schema.sql definition for users and watermarks tables
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);

CREATE TABLE IF NOT EXISTS watermarks (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(255) NOT NULL UNIQUE,
    last_exported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Idempotent seeding logic: Only seed if users table is empty
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM users LIMIT 1) THEN
        -- Seed 105,000 records
        INSERT INTO users (name, email, created_at, updated_at, is_deleted)
        SELECT 
            'User ' || i AS name,
            'user_' || i || '@example.com' AS email,
            -- created_at: spread realistically over the last 15 days
            NOW() - random() * INTERVAL '15 days' AS created_at,
            -- updated_at: initially set to created_at
            NOW() - random() * INTERVAL '15 days' AS updated_at,
            -- is_deleted: exactly 1.5% soft-deleted (which is >= 1% and >= 1,000 records)
            (CASE WHEN random() < 0.015 THEN TRUE ELSE FALSE END) AS is_deleted
        FROM generate_series(1, 105000) AS i;

        -- For realistic CDC patterns, set updated_at equal to or later than created_at
        UPDATE users 
        SET updated_at = created_at + random() * INTERVAL '3 days'
        WHERE updated_at < created_at;
    END IF;
END $$;

