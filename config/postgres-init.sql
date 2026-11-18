CREATE TABLE IF NOT EXISTS public.events (
    id          VARCHAR(128) PRIMARY KEY,
    event_type  VARCHAR(128) NOT NULL,
    payload     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
