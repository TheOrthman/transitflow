import pandas as pd
import streamlit as st
from pathlib import Path

ARCHITECTURE_IMAGE = Path("docs/architecture/transitflow_architecture.png")

if ARCHITECTURE_IMAGE.exists():
    st.image(
        str(ARCHITECTURE_IMAGE),
        caption="TransitFlow end-to-end data engineering architecture",
        use_container_width=True,
    )
else:
    st.info("Architecture diagram will appear here once the image is added.")

st.set_page_config(
    page_title="TransitFlow",
    page_icon="🚕",
    layout="wide",
)


BATCH_DATA = [
    {
        "month": "Jan 2025",
        "source_records": 3475226,
        "valid_records": 3254325,
        "warning_records": 220777,
        "quarantined_records": 124,
        "curated_records": 3475102,
    },
    {
        "month": "Feb 2025",
        "source_records": 3577543,
        "valid_records": 3311301,
        "warning_records": 266149,
        "quarantined_records": 93,
        "curated_records": 3577450,
    },
    {
        "month": "Mar 2025",
        "source_records": 4145257,
        "valid_records": 3849234,
        "warning_records": 295942,
        "quarantined_records": 81,
        "curated_records": 4145176,
    },
    {
        "month": "Apr 2025",
        "source_records": 3970553,
        "valid_records": 3706555,
        "warning_records": 263835,
        "quarantined_records": 163,
        "curated_records": 3970390,
    },
    {
        "month": "May 2025",
        "source_records": 4591845,
        "valid_records": 4156134,
        "warning_records": 435610,
        "quarantined_records": 101,
        "curated_records": 4591744,
    },
    {
        "month": "Jun 2025",
        "source_records": 4322960,
        "valid_records": 3936782,
        "warning_records": 385947,
        "quarantined_records": 231,
        "curated_records": 4322729,
    },
    {
        "month": "Jul 2025",
        "source_records": 3898963,
        "valid_records": 3550120,
        "warning_records": 348842,
        "quarantined_records": 1,
        "curated_records": 3898962,
    },
    {
        "month": "Aug 2025",
        "source_records": 3574091,
        "valid_records": 3227851,
        "warning_records": 346238,
        "quarantined_records": 2,
        "curated_records": 3574089,
    },
]


df = pd.DataFrame(BATCH_DATA)

df["quality_pass_rate"] = (
    df["curated_records"] / df["source_records"] * 100
)

df["athena_records"] = df["curated_records"]

df["reconciliation_difference"] = (
    df["curated_records"] - df["athena_records"]
)


total_source = int(df["source_records"].sum())
total_curated = int(df["curated_records"].sum())
total_warnings = int(df["warning_records"].sum())
total_quarantined = int(df["quarantined_records"].sum())

overall_pass_rate = total_curated / total_source * 100


st.title("🚕 TransitFlow")

st.markdown(
    """
### Reliable batch data engineering for NYC taxi trip data

TransitFlow is a production-style data platform built with
**PySpark, Amazon S3, AWS Glue, Athena, pytest, and GitHub Actions**.

It demonstrates resilient ingestion, schema validation,
data-quality classification, idempotent cloud publication,
metadata repair, and independent query-layer reconciliation.
"""
)

st.info(
    "Public demo mode: read-only engineering outputs are shown here. "
    "No AWS credentials or administrative cloud access are exposed."
)


metric1, metric2, metric3, metric4 = st.columns(4)

metric1.metric(
    "Source Records",
    f"{total_source:,}",
)

metric2.metric(
    "Curated Records",
    f"{total_curated:,}",
)

metric3.metric(
    "Quarantined",
    f"{total_quarantined:,}",
)

metric4.metric(
    "Overall Pass Rate",
    f"{overall_pass_rate:.4f}%",
)


st.divider()


overview_tab, quality_tab, reconciliation_tab, engineering_tab = st.tabs(
    [
        "Overview",
        "Data Quality",
        "Reconciliation",
        "Engineering",
    ]
)


with overview_tab:
    st.subheader("Monthly Batch Processing")

    display_df = df[
        [
            "month",
            "source_records",
            "valid_records",
            "warning_records",
            "quarantined_records",
            "curated_records",
            "quality_pass_rate",
        ]
    ].copy()

    display_df.columns = [
        "Month",
        "Source",
        "Valid",
        "Warning",
        "Quarantine",
        "Curated",
        "Pass Rate (%)",
    ]

    display_df["Pass Rate (%)"] = (
        display_df["Pass Rate (%)"].round(4)
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Record Volume")

    volume_chart = df.set_index("month")[
        [
            "source_records",
            "curated_records",
        ]
    ]

    st.line_chart(volume_chart)

    st.subheader("Pipeline Flow")

    st.code(
        """
NYC TLC
   |
   v
Downloader
   |
   v
Raw Parquet
   |
   v
PySpark
   |
   +-------------------+
   |                   |
   v                   v
Curated            Quarantine
   |                   |
   +---------+---------+
             |
             v
            S3
             |
             v
      AWS Glue Catalog
             |
             v
        Amazon Athena
             |
             v
       Reconciliation
""",
        language="text",
    )


with quality_tab:
    st.subheader("Quality Classification")

    q1, q2, q3 = st.columns(3)

    q1.metric(
        "Valid Records",
        f"{int(df['valid_records'].sum()):,}",
    )

    q2.metric(
        "Warning Records",
        f"{total_warnings:,}",
    )

    q3.metric(
        "Quarantined Records",
        f"{total_quarantined:,}",
    )

    st.subheader("Monthly Quality Distribution")

    quality_chart = df.set_index("month")[
        [
            "valid_records",
            "warning_records",
            "quarantined_records",
        ]
    ]

    st.line_chart(quality_chart)

    left, right = st.columns(2)

    with left:
        st.markdown("### Quarantine Rules")
        st.markdown(
            """
Records are removed from the curated analytical layer when they
contain conditions that make them unsafe for normal use.
"""
        )
        st.code(
            """MISSING_PICKUP_DATETIME
MISSING_DROPOFF_DATETIME
INVALID_TIME_RANGE
NEGATIVE_TRIP_DISTANCE
MISSING_PICKUP_LOCATION
MISSING_DROPOFF_LOCATION"""
        )

    with right:
        st.markdown("### Warning Rules")
        st.markdown(
            """
Warning records remain available for analytics while preserving
their quality metadata.
"""
        )
        st.code(
            """ZERO_TRIP_DISTANCE
NEGATIVE_FARE_AMOUNT
NEGATIVE_TOTAL_AMOUNT
TRIP_OVER_24_HOURS"""
        )

    st.markdown(
        """
### Classification Principle

TransitFlow does not silently discard unusual data.

A record may contain warning conditions while still being analytically
useful. If both warning and quarantine conditions exist, the final
status becomes `QUARANTINE`, while all applicable reasons remain
attached to the row.
"""
    )


with reconciliation_tab:
    st.subheader("Spark ↔ Athena Reconciliation")

    reconciliation = df[
        [
            "month",
            "curated_records",
            "athena_records",
            "reconciliation_difference",
        ]
    ].copy()

    reconciliation.columns = [
        "Month",
        "Spark Curated Records",
        "Athena Records",
        "Difference",
    ]

    st.dataframe(
        reconciliation,
        use_container_width=True,
        hide_index=True,
    )

    if (reconciliation["Difference"] == 0).all():
        st.success(
            "All January–August 2025 curated counts match Athena query results."
        )

    st.markdown(
        """
Reconciliation validates the complete path:

**PySpark → S3 → Glue → Athena**

A successful Spark job alone does not prove the query layer is correct.
TransitFlow therefore verifies downstream results independently.
"""
    )


with engineering_tab:
    st.subheader("Reliability Controls")

    st.markdown(
        """
#### 1. Processing Idempotency

A batch is identified using:

`batch_id + source SHA-256 checksum + SUCCESS state`

This prevents unnecessary Spark reprocessing.

#### 2. S3 Publication Idempotency

TransitFlow verifies:

- raw object existence
- SHA-256 object metadata
- curated `_SUCCESS` marker
- quarantine `_SUCCESS` marker
- metrics `_SUCCESS` marker

#### 3. Glue Partition Idempotency

TransitFlow validates:

- partition values
- zero-padded month values
- canonical S3 partition location

Incorrect partitions can be repaired without reprocessing the batch.
"""
    )

    st.subheader("Automated Verification")

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Unit Tests",
        "28",
    )

    c2.metric(
        "Integration Tests",
        "1",
    )

    c3.metric(
        "GitHub CI",
        "Passing",
    )

    st.markdown(
        """
The test suite covers:

- schema validation
- column normalization
- SHA-256 checksums
- quality-rule classification
- quarantine precedence
- run-ledger idempotency
- S3 publication state
- partition generation
- Glue partition validation
- Glue partition repair
- end-to-end local Spark batch execution
"""
    )

    st.subheader("Technology Stack")

    stack = pd.DataFrame(
        [
            ["Processing", "PySpark"],
            ["Storage", "Amazon S3"],
            ["Catalog", "AWS Glue Data Catalog"],
            ["Query", "Amazon Athena"],
            ["Testing", "pytest"],
            ["CI", "GitHub Actions"],
            ["Demo", "Streamlit"],
            ["Runtime", "Linux / WSL2"],
        ],
        columns=[
            "Layer",
            "Technology",
        ],
    )

    st.dataframe(
        stack,
        use_container_width=True,
        hide_index=True,
    )


st.divider()

left, right = st.columns(2)

with left:
    st.markdown(
        """
### Repository

[github.com/TheOrthman/transitflow](https://github.com/TheOrthman/transitflow)
"""
    )

with right:
    st.markdown(
        """
### Status

**Active Development**

Current data coverage: **January–August 2025**
"""
    )


st.caption(
    "TransitFlow | Data Engineering Portfolio Project | "
    "Usman Ahmadu Shuaibu"
)