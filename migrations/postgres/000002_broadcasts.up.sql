-- Broadcasts to a segment of users (Botsight.md, 22): the dashboard creates one, the SDK
-- inside the bot leases recipients, sends the messages itself and reports the results.

CREATE TABLE broadcasts (
    id           SERIAL PRIMARY KEY,
    project_id   INT    NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    text         TEXT   NOT NULL CHECK (char_length(text) BETWEEN 1 AND 4096),
    parse_mode   TEXT   NOT NULL DEFAULT '' CHECK (parse_mode IN ('', 'HTML', 'MarkdownV2')),
    segment      JSONB  NOT NULL,
    status       TEXT   NOT NULL DEFAULT 'sending' CHECK (status IN ('sending', 'done', 'canceled')),
    total        INT    NOT NULL,
    created_by   BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

CREATE INDEX broadcasts_sending ON broadcasts (project_id, id) WHERE status = 'sending';

-- One row per recipient. A pending row is free when leased_until is NULL or in the past:
-- a bot that died with a lease gives its recipients back by itself.
CREATE TABLE broadcast_recipients (
    broadcast_id  INT      NOT NULL REFERENCES broadcasts(id) ON DELETE CASCADE,
    chat_id       BIGINT   NOT NULL,
    status        TEXT     NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'blocked', 'failed')),
    leased_until  TIMESTAMPTZ,
    attempts      SMALLINT NOT NULL DEFAULT 0,
    error         TEXT     NOT NULL DEFAULT '',
    updated_at    TIMESTAMPTZ,
    PRIMARY KEY (broadcast_id, chat_id)
);

CREATE INDEX broadcast_recipients_pending ON broadcast_recipients (broadcast_id, chat_id) WHERE status = 'pending';
