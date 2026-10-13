-- Events written by ch-writer from Kafka (Botsight.md, 12.1-12.3).
-- Statements are separated by semicolons, so comments and strings must not contain them.

CREATE TABLE IF NOT EXISTS events
(
    project_id     UInt32,
    bot_id         UInt64,
    event_id       UUID,
    ts             DateTime64(3, 'UTC'),
    received_at    DateTime64(3, 'UTC'),
    clock_skewed   UInt8,
    type           LowCardinality(String),
    name           LowCardinality(String),
    user_id        UInt64,
    chat_id        Int64,
    chat_type      LowCardinality(String),
    content_type   LowCardinality(String),
    command        LowCardinality(String),
    start_param    String,
    callback_data  String,
    state          LowCardinality(String),
    handler        LowCardinality(String),
    state_from     LowCardinality(String),
    state_to       LowCardinality(String),
    member_status  LowCardinality(String),
    handled        UInt8,
    ok             UInt8,
    duration_ms    UInt32,
    error_code     UInt16,
    error          LowCardinality(String),
    text_len       UInt16,
    user_lang      LowCardinality(String),
    is_premium     UInt8,
    has_username   UInt8,
    props_str      Map(LowCardinality(String), String),
    props_num      Map(LowCardinality(String), Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (project_id, bot_id, toDate(ts), user_id, ts)
TTL toDateTime(ts) + INTERVAL 13 MONTH
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS messages_text
(
    project_id  UInt32,
    bot_id      UInt64,
    event_id    UUID,
    ts          DateTime64(3, 'UTC'),
    user_id     UInt64,
    state       LowCardinality(String),
    handled     UInt8,
    text        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (project_id, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 1000;

-- One row per accepted batch: SDK versions and events lost inside the SDK (dropped).
CREATE TABLE IF NOT EXISTS ingest_batches
(
    project_id         UInt32,
    batch_id           UUID,
    received_at        DateTime64(3, 'UTC'),
    sdk_name           LowCardinality(String),
    sdk_version        LowCardinality(String),
    framework          LowCardinality(String),
    framework_version  LowCardinality(String),
    events             UInt32,
    rejected           UInt32,
    dropped            UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (project_id, received_at)
TTL toDateTime(received_at) + INTERVAL 13 MONTH
SETTINGS non_replicated_deduplication_window = 1000;

-- Views write into their target with a token derived from the source insert, so the
-- target needs its own deduplication window: a repeated chunk is not counted twice.
CREATE TABLE IF NOT EXISTS events_daily
(
    project_id  UInt32,
    bot_id      UInt64,
    day         Date,
    type        LowCardinality(String),
    name        LowCardinality(String),
    events      SimpleAggregateFunction(sum, UInt64),
    users       AggregateFunction(uniqCombined, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, bot_id, day, type, name)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS events_daily_mv TO events_daily AS
SELECT
    project_id, bot_id, toDate(ts) AS day, type, name,
    count() AS events,
    uniqCombinedState(user_id) AS users
FROM events
GROUP BY project_id, bot_id, day, type, name;

CREATE TABLE IF NOT EXISTS user_first_start
(
    project_id   UInt32,
    user_id      UInt64,
    first_start  SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    source       AggregateFunction(argMin, String, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, user_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_first_start_mv TO user_first_start AS
SELECT project_id, user_id, min(ts) AS first_start, argMinState(start_param, ts) AS source
FROM events
WHERE type = 'update' AND command = 'start'
GROUP BY project_id, user_id;
