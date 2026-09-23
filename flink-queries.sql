-- ============================================================
-- Multi-Region Compliance-Aware Data Router
-- Flink SQL Statements — Confluent Cloud
-- ============================================================
-- These statements implement the real-time compliance engine:
-- each event is checked against a region-specific PII policy
-- as it streams through, and tagged with a status and reason
-- before being merged into a single governed output topic.
-- ============================================================


-- ------------------------------------------------------------
-- Step 1: Register raw source streams
-- (Flink auto-discovers Kafka topics as tables once Schema
-- Registry is linked, but these SELECTs confirm the shape.)
-- ------------------------------------------------------------

SELECT * FROM `events.eu` LIMIT 10;

SELECT * FROM `events.us` LIMIT 10;


-- ------------------------------------------------------------
-- Step 2: EU compliance check
-- Rule: ANY raw_email OR raw_phone present = violation
-- (EU policy is strict — no raw PII permitted downstream)
-- ------------------------------------------------------------

CREATE TABLE eu_checked AS
SELECT *,
  CASE
    WHEN raw_email IS NOT NULL OR raw_phone IS NOT NULL
    THEN 'PII_IN_EU_EVENT'
    ELSE 'OK'
  END AS violation_reason,
  CASE
    WHEN raw_email IS NOT NULL OR raw_phone IS NOT NULL
    THEN 'quarantine'
    ELSE 'clean'
  END AS status
FROM `events.eu`;


-- ------------------------------------------------------------
-- Step 3: US compliance check
-- Rule: ONLY raw_email present = violation
-- (US policy is more lenient — phone numbers are permitted)
-- ------------------------------------------------------------

CREATE TABLE us_checked AS
SELECT *,
  CASE
    WHEN raw_email IS NOT NULL
    THEN 'PII_EMAIL_IN_US_EVENT'
    ELSE 'OK'
  END AS violation_reason,
  CASE
    WHEN raw_email IS NOT NULL
    THEN 'quarantine'
    ELSE 'clean'
  END AS status
FROM `events.us`;


-- ------------------------------------------------------------
-- Step 4: Merge both regional streams into one governed output
-- This is the single topic the Elasticsearch sink connector
-- (and dashboard) reads from.
-- ------------------------------------------------------------

CREATE TABLE all_routed_events AS
SELECT user_id, region, event_type, `timestamp`, status, violation_reason
FROM eu_checked
UNION ALL
SELECT user_id, region, event_type, `timestamp`, status, violation_reason
FROM us_checked;


-- ------------------------------------------------------------
-- Verification queries (used to validate the pipeline live)
-- ------------------------------------------------------------

-- View only quarantined (non-compliant) events
SELECT * FROM all_routed_events WHERE status = 'quarantine' LIMIT 20;

-- View only clean (compliant) events
SELECT * FROM all_routed_events WHERE status = 'clean' LIMIT 20;

-- Aggregate compliance summary
SELECT status, COUNT(*) AS cnt
FROM all_routed_events
GROUP BY status;

-- Result observed during verification run:
--   quarantine : 2927
--   clean      : 2011
