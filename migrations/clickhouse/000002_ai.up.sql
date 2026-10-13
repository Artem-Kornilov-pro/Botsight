-- AI module (Botsight.md, 14).
-- Statements are separated by semicolons, so comments and strings must not contain them.

-- Which vector (unique normalized text) every message maps to. Written by ai-worker.
CREATE TABLE IF NOT EXISTS message_points
(
    project_id  UInt32,
    event_id    UUID,
    ts          DateTime64(3, 'UTC'),
    point_id    UUID
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (project_id, ts, event_id)
TTL toDateTime(ts) + INTERVAL 30 DAY;

-- Topic of every vector from the latest clustering run (the row with the latest assigned_at wins).
CREATE TABLE IF NOT EXISTS point_topics
(
    project_id   UInt32,
    point_id     UUID,
    topic_id     UInt32,
    assigned_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(assigned_at)
ORDER BY (project_id, point_id)
TTL toDateTime(assigned_at) + INTERVAL 30 DAY;

-- Topics with names and trends, one current row per topic (read with FINAL).
CREATE TABLE IF NOT EXISTS topics
(
    project_id         UInt32,
    topic_id           UInt32,
    name               String,
    description        String,
    keywords           Array(String),
    size               UInt32,
    messages_7d        UInt32,
    messages_prev_7d   UInt32,
    share_7d           Float64,
    share_prev_7d      Float64,
    growth             Float64,
    example_event_ids  Array(UUID),
    centroid           Array(Float32),
    active             UInt8,
    named_by           LowCardinality(String),
    updated_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, topic_id);

-- Alerts from the alerts topic, written by ch-writer: topic trends and anomalies.
CREATE TABLE IF NOT EXISTS alerts
(
    project_id  UInt32,
    alert_id    UUID,
    kind        LowCardinality(String),
    created_at  DateTime64(3, 'UTC'),
    title       String,
    text        String,
    details     String
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (project_id, alert_id)
TTL toDateTime(created_at) + INTERVAL 13 MONTH
SETTINGS non_replicated_deduplication_window = 1000;

-- Limits for SQL written by the LLM (Botsight.md, 14.2). Users nl_p{project_id} get this profile.
CREATE SETTINGS PROFILE IF NOT EXISTS nl_readonly SETTINGS
    readonly = 1,
    max_execution_time = 10,
    max_result_rows = 10000,
    result_overflow_mode = 'break',
    max_rows_to_read = 500000000,
    max_memory_usage = 2000000000,
    max_threads = 4;
