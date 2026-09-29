# ADR 002: Distinguish Warning Records from Quarantine Records

## Status

Accepted

## Context

Not every unusual source value should automatically be treated as invalid.

Some records contain conditions that make them unsafe for normal analytical use, such as:

- missing pickup datetime
- missing dropoff datetime
- dropoff before pickup
- negative trip distance
- missing pickup location
- missing dropoff location

Other records may be unusual but still represent legitimate source-system behavior.

Examples include:

- zero trip distance
- negative fare amount
- negative total amount
- trip duration above 24 hours

Automatically discarding all unusual records would remove potentially meaningful source behavior and make data-quality decisions harder to audit.

---

## Decision

TransitFlow classifies records into three statuses:

```text
VALID
WARNING
QUARANTINE
