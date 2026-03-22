-- Migration to add optimized income caching fields
ALTER TABLE users ADD COLUMN IF NOT EXISTS income_per_minute BIGINT DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_income_at DATETIME DEFAULT CURRENT_TIMESTAMP;

-- Add index for high-performance group updates
CREATE INDEX IF NOT EXISTS ix_users_income_per_minute ON users (income_per_minute);
