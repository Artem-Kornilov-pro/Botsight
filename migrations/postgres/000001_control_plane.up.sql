-- Control plane: accounts, projects, access, API keys and dashboard data (Botsight.md, 12.4).

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE accounts (
    id                 BIGSERIAL PRIMARY KEY,
    email              TEXT UNIQUE,                        -- lower case; NULL for Telegram-only accounts
    email_verified_at  TIMESTAMPTZ,
    password_hash      TEXT,                               -- argon2id; NULL when signing in via Telegram only
    telegram_id        BIGINT UNIQUE,
    telegram_username  TEXT,
    name               TEXT NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_has_login CHECK (email IS NOT NULL OR telegram_id IS NOT NULL),
    CONSTRAINT accounts_email_lower CHECK (email = lower(email)),
    CONSTRAINT accounts_password_needs_email CHECK (password_hash IS NULL OR email IS NOT NULL)
);

-- One-time links from emails: email confirmation and password reset.
CREATE TABLE account_tokens (
    token_hash  BYTEA PRIMARY KEY,                         -- SHA-256 of the token from the link
    account_id  BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    purpose     TEXT NOT NULL CHECK (purpose IN ('verify_email', 'reset_password')),
    email       TEXT NOT NULL,                             -- address the link was sent to
    expires_at  TIMESTAMPTZ NOT NULL,
    used_at     TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX account_tokens_account ON account_tokens (account_id, purpose);

CREATE TABLE projects (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    bot_id      BIGINT,                                    -- filled by the first event
    plan        TEXT NOT NULL DEFAULT 'free',
    settings    JSONB NOT NULL DEFAULT '{}',               -- text capture, neutral handlers, ...
    created_by  BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Owners are members with role 'owner'; a project may have several.
CREATE TABLE project_members (
    project_id  INT    NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    account_id  BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    role        TEXT   NOT NULL CHECK (role IN ('owner', 'editor', 'viewer')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, account_id)
);
CREATE INDEX project_members_account ON project_members (account_id);

CREATE TABLE api_keys (
    id           SERIAL PRIMARY KEY,
    project_id   INT    NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    prefix       TEXT   NOT NULL UNIQUE,                   -- bs_live_ab12cd34
    secret_hash  BYTEA  NOT NULL,                          -- SHA-256 of the full key
    name         TEXT   NOT NULL DEFAULT '',
    created_by   BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at   TIMESTAMPTZ
);
CREATE INDEX api_keys_project ON api_keys (project_id);

CREATE TABLE saved_funnels (
    id          SERIAL PRIMARY KEY,
    project_id  INT    NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name        TEXT   NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    definition  JSONB  NOT NULL,                           -- steps, window, breakdown
    created_by  BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX saved_funnels_project ON saved_funnels (project_id);

CREATE TABLE ad_spend (
    id          SERIAL PRIMARY KEY,
    project_id  INT     NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source      TEXT    NOT NULL CHECK (length(source) BETWEEN 1 AND 64),  -- deep link label
    period      DATERANGE NOT NULL CHECK (NOT isempty(period)),
    amount_rub  NUMERIC(12, 2) NOT NULL CHECK (amount_rub >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- spend of one source is never counted twice for the same day
    CONSTRAINT ad_spend_no_overlap EXCLUDE USING gist (project_id WITH =, source WITH =, period WITH &&)
);
