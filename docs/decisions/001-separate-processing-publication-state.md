# ADR 001: Separate Processing State from Publication State

## Status

Accepted

## Context

A data batch may be successfully processed locally while one or more cloud
publication steps fail.

Examples include:

- temporary AWS authentication failure
- incomplete S3 upload
- missing `_SUCCESS` marker
- incorrect Glue partition location
- Glue registration failure after successful Spark processing

Treating processing and publication as one single state would force expensive
Spark work to be repeated even when the transformation itself already
completed successfully.

---

## Decision

TransitFlow treats processing state and cloud publication state independently.

Processing success is tracked through:

```text
batch_id
source_checksum
SUCCESS status
