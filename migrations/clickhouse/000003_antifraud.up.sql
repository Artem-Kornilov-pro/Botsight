-- Antifraud (Botsight.md, 15).
-- Statements are separated by semicolons, so comments and strings must not contain them.

-- Behaviour of a user over the stream window, written by antifraud (the latest row wins).
-- The traffic source is not stored here: it comes from user_first_start.
CREATE TABLE IF NOT EXISTS user_features
(
    project_id          UInt32,
    user_id             UInt64,
    first_event         DateTime64(3, 'UTC'),
    last_seen           DateTime64(3, 'UTC'),
    updates             UInt32,
    intervals           UInt32,
    fast_intervals      UInt32,
    median_interval_ms  UInt32,
    signature           String,   -- first intervals between messages in 100 ms buckets, e.g. 1.1.1.1.1
    has_username        UInt8,
    is_premium          UInt8,
    lang                LowCardinality(String),
    updated_at          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, user_id)
TTL toDateTime(last_seen) + INTERVAL 30 DAY;

-- Members of suspicious referral components, written by the graph job.
CREATE TABLE IF NOT EXISTS referral_flags
(
    project_id      UInt32,
    user_id         UInt64,
    component_size  UInt32,
    updated_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, user_id)
TTL toDateTime(updated_at) + INTERVAL 7 DAY;

-- Suspicious users of the latest scoring run (rows of one run share updated_at).
CREATE TABLE IF NOT EXISTS user_risk
(
    project_id  UInt32,
    user_id     UInt64,
    source      String,
    joined      DateTime64(3, 'UTC'),
    score       Float32,
    reasons     Array(LowCardinality(String)),
    updated_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, user_id)
TTL toDateTime(updated_at) + INTERVAL 30 DAY;

-- Share of suspicious users per traffic source over the last 24 hours, per scoring run.
CREATE TABLE IF NOT EXISTS source_risk
(
    project_id  UInt32,
    source      String,
    users       UInt32,
    suspicious  UInt32,
    avg_score   Float32,
    reasons     Map(LowCardinality(String), UInt32),
    updated_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, source)
TTL toDateTime(updated_at) + INTERVAL 30 DAY;
