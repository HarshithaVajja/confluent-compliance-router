# Multi-Region Compliance-Aware Data Router

A real-time data governance pipeline built on Confluent Cloud, demonstrating how streaming infrastructure can enforce regional data-compliance rules (GDPR-style) as data flows — not after the fact.

## The Problem

Companies operating across regions (EU, US, etc.) must ensure that raw personally identifiable information (PII) — email addresses, phone numbers — does not flow downstream in violation of regional privacy policy. Manually auditing this after the fact is slow and reactive. This project enforces the policy **in-flight**, at the stream level, with full traceability.

## Compliance Rules

| Region | Rule |
|---|---|
| EU | Any event containing a raw email **or** raw phone number is quarantined |
| US | Only a raw email triggers quarantine — phone numbers are permitted |

Violations are not manufactured or pre-labeled — the underlying event generator (Datagen) populates PII fields randomly across both regions, so the compliance engine has to genuinely detect violations rather than filter on a field it controls.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│ Datagen: EU      │     │ Datagen: US      │      ← Source Connectors
└────────┬─────────┘     └────────┬─────────┘
         ↓                        ↓
   topic: events.eu         topic: events.us       ← Schema Registry-validated (Avro)
         ↓                        ↓
┌───────────────────────────────────────────┐
│         Flink SQL — compliance logic         │      ← Stream Processing
│  (per-region rule → violation_reason field) │
└───────────────────────────────────────────┘
         ↓                        ↓
    eu_checked                us_checked
         ↓                        ↓
┌───────────────────────────────────────────┐
│     topic: all_routed_events                │      ← Merged, tagged output
│  (status: clean/quarantine + reason)        │
└───────────────────────────────────────────┘
                    ↓
        Elasticsearch Sink V2 Connector          ← configured (see Known Limitations)
                    ↓
       Stream Catalog + Stream Lineage           ← Governance layer
```

## Confluent Components Used

- **Source Connectors:** 2x Datagen Source connectors generating realistic, schema-validated EU/US event streams
- **Schema Registry:** Avro schema with nullable `raw_email`/`raw_phone` fields, enabling genuine (not pre-labeled) PII violations
- **Stream Processing (Flink SQL):** Per-region compliance rule evaluation, tagging each event with `status` and `violation_reason`, then merging both regional streams into one governed output topic
- **Sink Connector:** Elasticsearch Sink V2, configured with API-key authentication against an Elastic Cloud serverless project (see Known Limitations)
- **Stream Governance:** Stream Lineage traces the full data path end-to-end, from source connector through Flink transformations to the sink

## Results (Verified)

Running the live compliance query against real generated data:

```sql
SELECT status, COUNT(*) as cnt FROM all_routed_events GROUP BY status;
```

- **2,927 events quarantined** (PII violations correctly detected)
- **2,011 events routed clean**

Sample quarantined records, showing the violation reason attached inline:

| user_id | region | event_type | status | violation_reason |
|---|---|---|---|---|
| u1005 | eu | purchase | quarantine | PII_IN_EU_EVENT |
| u1001 | eu | support_ticket | quarantine | PII_IN_EU_EVENT |
| u1004 | eu | login | quarantine | PII_IN_EU_EVENT |

## Governance in Practice

**Stream Lineage** provides a full, automatically-generated visual trace of the pipeline: from `events.eu`/`events.us` source topics, through each Flink transformation stage, to the final merged `all_routed_events` topic and downstream sink — proving the compliance decision path is fully auditable, not a black box.

## Known Limitations

The Elasticsearch Sink V2 connector is fully and correctly configured (Connection URI, AVRO input format, API-key authentication, SSL enabled — see connector configuration screenshot). Write access to the target Elastic Cloud serverless deployment was independently verified via direct `curl` requests using the same API key, confirming the credentials and endpoint are valid. However, the connector itself did not complete successful provisioning within the project's time constraints; the root cause was not fully isolated (Confluent's connector-level logs did not surface a specific exception beyond a generic retry-exhaustion message). Given this, the project demonstrates the compliance and governance logic through direct Flink SQL query results and the Stream Lineage graph rather than a live Elasticsearch dashboard.

## What This Demonstrates

- Genuine, non-trivial stream processing logic (not just pass-through aggregation)
- Real Confluent connectors on both ingestion and (configured) egress sides
- Governance as a structural requirement of the pipeline, not a bolted-on feature
- Honest documentation of what works, what's configured-but-unverified, and why
