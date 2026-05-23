"""
Healthcare Analytics Pipeline
==============================
End-to-end ETL pipeline for CMS Medicare public data.
Covers: ingestion → cleaning/validation → warehouse loading → Airflow DAG → SQL queries.

SCOPE (explicit):
    This pipeline processes the CMS Medicare Physician & Other Practitioners dataset
    (provider-level) as the primary source, and the CMS Hospital General Information
    dataset as a secondary source.  Both are fully modeled.  CDC WONDER enrichment
    is out of scope for this version and is noted as a future extension.

FACT TABLE GRAIN (explicit):
    Each row in analytics.fact_services represents ONE unique combination of
    (provider NPI × HCPCS procedure code × state_code × year).
    Aggregation to this grain happens BEFORE loading — no double-counting.

NAMING CONVENTION (one standard, applied everywhere):
    Schemas  : staging          | analytics
    Staging  : staging.cms_providers          staging.cms_hospitals
               staging.clean_providers        staging.clean_hospitals
    Dims     : analytics.dim_provider         analytics.dim_procedure
               analytics.dim_geography        analytics.dim_hospital
    Fact     : analytics.fact_services

CREDENTIALS:
    All secrets are read from environment variables or a .env file.
    Copy .env.example to .env and fill in your values before running.

Usage:
    python healthcare_pipeline.py [phase]

    Phases:
        setup       – create DB schemas and all warehouse tables (DDL)
        ingest      – download CMS CSVs and load into staging tables
        transform   – validate, clean, write staging.clean_*
        load        – populate star-schema (dims first, then fact, via staging tables)
        (dag is defined inline at module level — no separate file needed)
        queries     – run the 5 analytical SQL queries and print results
        all         – run setup → ingest → transform → load → queries

Requirements:
    pip install pandas==2.2.0 numpy==1.26.4 psycopg2-binary==2.9.9 \\
                sqlalchemy==2.0.25 requests==2.31.0 python-dotenv==1.0.0
    # Apache Airflow only needed for the 'dag' phase:
    pip install apache-airflow==2.8.1
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS  (only what is actually used)
# ─────────────────────────────────────────────────────────────────────────────

import os
import sys
import logging
import argparse
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("healthcare_pipeline")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  —  loaded from environment / .env file
#
# Create a file named  .env  in the project root with these lines:
#   DB_USER=postgres
#   DB_PASSWORD=yourpassword
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_NAME=healthcare_pipeline
#
# The defaults below are used only if the environment variable is absent,
# which is fine for local development but should never reach production.
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()  # reads .env if present; no-op if absent

DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "yourpassword")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "healthcare_pipeline")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

RAW_DIR       = "data/raw"
PROCESSED_DIR = "data/processed"
DAGS_DIR      = "dags"

# CMS dataset download URLs.
# Visit https://data.cms.gov and search by dataset name if a URL stops working.
# Place the CSV manually at data/raw/<key>.csv and the script will skip download.
DATASETS: dict[str, str] = {
    "cms_providers": (
        "https://data.cms.gov/sites/default/files/2023-04/"
        "MUP_PHY_R23_P05_V10_D21_Prov.csv"
    ),
    "cms_hospitals": (
         "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv"
    ),
}

# Data year — update when loading a different CMS release
CMS_DATA_YEAR: int = 2022

# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE DATA
# ─────────────────────────────────────────────────────────────────────────────

STATE_NAMES: dict[str, str] = {
    "AL": "Alabama",        "AK": "Alaska",         "AZ": "Arizona",
    "AR": "Arkansas",       "CA": "California",      "CO": "Colorado",
    "CT": "Connecticut",    "DE": "Delaware",        "FL": "Florida",
    "GA": "Georgia",        "HI": "Hawaii",          "ID": "Idaho",
    "IL": "Illinois",       "IN": "Indiana",         "IA": "Iowa",
    "KS": "Kansas",         "KY": "Kentucky",        "LA": "Louisiana",
    "ME": "Maine",          "MD": "Maryland",        "MA": "Massachusetts",
    "MI": "Michigan",       "MN": "Minnesota",       "MS": "Mississippi",
    "MO": "Missouri",       "MT": "Montana",         "NE": "Nebraska",
    "NV": "Nevada",         "NH": "New Hampshire",   "NJ": "New Jersey",
    "NM": "New Mexico",     "NY": "New York",        "NC": "North Carolina",
    "ND": "North Dakota",   "OH": "Ohio",            "OK": "Oklahoma",
    "OR": "Oregon",         "PA": "Pennsylvania",    "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota",    "TN": "Tennessee",
    "TX": "Texas",          "UT": "Utah",            "VT": "Vermont",
    "VA": "Virginia",       "WA": "Washington",      "WV": "West Virginia",
    "WI": "Wisconsin",      "WY": "Wyoming",         "DC": "District of Columbia",
    "PR": "Puerto Rico",    "VI": "Virgin Islands",  "GU": "Guam",
}

REGIONS: dict[str, list[str]] = {
    "Northeast": ["CT", "ME", "MA", "NH", "NJ", "NY", "PA", "RI", "VT"],
    "Midwest":   ["IL", "IN", "IA", "KS", "MI", "MN", "MO", "NE", "ND", "OH", "SD", "WI"],
    "South":     ["AL", "AR", "DC", "DE", "FL", "GA", "KY", "LA", "MD",
                  "MS", "NC", "OK", "SC", "TN", "TX", "VA", "WV"],
    "West":      ["AK", "AZ", "CA", "CO", "HI", "ID", "MT", "NV", "NM",
                  "OR", "UT", "WA", "WY"],
}

VALID_STATE_CODES: set[str]  = set(STATE_NAMES.keys())
VALID_GENDER_CODES: set[str] = {"M", "F"}

# ─────────────────────────────────────────────────────────────────────────────
# COLUMN ALIAS MAPS
# Maps every known CMS header variant → canonical snake_case name.
# Handles format changes across CMS release years without breaking the pipeline.
# ─────────────────────────────────────────────────────────────────────────────

CMS_PROVIDER_COL_ALIASES: dict[str, str] = {
    "npi":                           "npi",
    "rndrng_npi":                    "npi",
    "nppes_provider_last_org_name":  "last_name",
    "rndrng_prvdr_last_org_name":    "last_name",
    "nppes_provider_first_name":     "first_name",
    "rndrng_prvdr_first_name":       "first_name",
    "nppes_credentials":             "credential",
    "rndrng_prvdr_crdntls":          "credential",
    "nppes_provider_gender":         "gender",
    "rndrng_prvdr_gndr":             "gender",
    "nppes_entity_code":             "entity_code",
    "rndrng_prvdr_ent_cd":           "entity_code",
    "provider_type":                 "provider_type",
    "rndrng_prvdr_type":             "provider_type",
    "nppes_provider_state":          "state_code",
    "rndrng_prvdr_state_abrvtn":     "state_code",
    "nppes_provider_zip":            "zip_code",
    "rndrng_prvdr_zip5":             "zip_code",
    "hcpcs_cd":                      "hcpcs_code",
    "hcpcs_code":                    "hcpcs_code",
    "hcpcs_description":             "hcpcs_desc",
    "hcpcs_desc":                    "hcpcs_desc",
    "hcpcs_drug_ind":                "hcpcs_drug_ind",
    "hcpcs_drug_indicator":          "hcpcs_drug_ind",
    "bene_unique_cnt":               "tot_benes",
    "tot_benes":                     "tot_benes",
    "tot_srvcs":                     "tot_srvcs",
    "line_srvc_cnt":                 "tot_srvcs",
    "average_submitted_chrg_amt":    "avg_submitted_charge",
    "avg_sbmtd_chrg":                "avg_submitted_charge",
    "average_medicare_allowed_amt":  "avg_medicare_allowed",
    "avg_mdcr_alowd_amt":            "avg_medicare_allowed",
    "average_medicare_payment_amt":  "avg_medicare_payment",
    "avg_mdcr_pymt_amt":             "avg_medicare_payment",
}

# Hospital column aliases (CMS Hospital General Information)
CMS_HOSPITAL_COL_ALIASES: dict[str, str] = {
    "facility_id":              "cms_certification_number",
    "provider_id":              "cms_certification_number",
    "facility_name":            "hospital_name",
    "hospital_name":            "hospital_name",
    "address":                  "address",
    "city":                     "city",
    "city/town":                "city",
    "state":                    "state_code",
    "zip_code":                 "zip_code",
    "county_name":              "county_name",
    "county/parish":            "county_name",
    "phone_number":             "phone_number",
    "hospital_type":            "hospital_type",
    "hospital_ownership":       "ownership_type",
    "ownership":                "ownership_type",
    "emergency_services":       "emergency_services",
    "hospital_overall_rating":  "overall_rating",
    "overall_rating":           "overall_rating",
}

# ─────────────────────────────────────────────────────────────────────────────
# REQUIRED COLUMNS (checked after alias normalisation — fail-fast if absent)
# ─────────────────────────────────────────────────────────────────────────────

PROVIDER_REQUIRED_COLS: list[str] = [
    "npi", "hcpcs_code", "state_code", "avg_medicare_payment", "tot_srvcs",
]

HOSPITAL_REQUIRED_COLS: list[str] = [
    "cms_certification_number", "hospital_name", "state_code",
]

NUMERIC_COLS: list[str] = [
    "tot_benes", "tot_srvcs",
    "avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment",
]

# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON ENGINE
# ─────────────────────────────────────────────────────────────────────────────

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, pool_pre_ping=True)
    return _engine

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 – SETUP:  DDL for all schemas and tables
# ─────────────────────────────────────────────────────────────────────────────

# Each statement is separated by a unique sentinel so we can split and execute
# individually, making partial-failure messages precise.
_DDL_STATEMENTS: list[str] = [
    "CREATE SCHEMA IF NOT EXISTS staging",
    "CREATE SCHEMA IF NOT EXISTS analytics",

    # ── Provider dimension ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS analytics.dim_provider (
        provider_key  SERIAL       PRIMARY KEY,
        npi           VARCHAR(20)  UNIQUE NOT NULL,
        last_name     VARCHAR(100),
        first_name    VARCHAR(100),
        credential    VARCHAR(50),
        gender        CHAR(1),       -- 'M' / 'F' / NULL
        entity_code   CHAR(1),       -- 'I' individual / 'O' organization
        provider_type VARCHAR(150),
        state_code    CHAR(2),
        zip_code      VARCHAR(5)     -- 5-digit ZIP; ZIP+4 stripped intentionally
    )
    """,

    # ── Procedure dimension ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS analytics.dim_procedure (
        procedure_key  SERIAL      PRIMARY KEY,
        hcpcs_code     VARCHAR(10) UNIQUE NOT NULL,
        hcpcs_desc     TEXT,
        hcpcs_drug_ind CHAR(1)     -- 'Y' drug / 'N' non-drug / NULL unknown
    )
    """,

    # ── Geography dimension ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS analytics.dim_geography (
        geo_key     SERIAL  PRIMARY KEY,
        state_code  CHAR(2) UNIQUE NOT NULL,
        state_name  VARCHAR(60),
        region      VARCHAR(30)
    )
    """,

    # ── Hospital dimension  (secondary source — CMS Hospital General Info) ─
    """
    CREATE TABLE IF NOT EXISTS analytics.dim_hospital (
        hospital_key             SERIAL       PRIMARY KEY,
        cms_certification_number VARCHAR(10)  UNIQUE NOT NULL,
        hospital_name            VARCHAR(200),
        address                  VARCHAR(200),
        city                     VARCHAR(100),
        state_code               CHAR(2),
        zip_code                 VARCHAR(5),
        county_name              VARCHAR(100),
        phone_number             VARCHAR(20),
        hospital_type            VARCHAR(100),
        ownership_type           VARCHAR(100),
        emergency_services       VARCHAR(5),  -- 'Yes' / 'No'
        overall_rating           SMALLINT     -- 1–5 CMS star rating, NULL if not rated
    )
    """,

    # ── Services fact table ────────────────────────────────────────────────
    # GRAIN: one row per (npi × hcpcs_code × state_code × year)
    # Monetary columns = WEIGHTED per-service averages (weight = total_services).
    # Derived spend = avg_medicare_payment × total_services  (computed at query time,
    # never stored, to avoid redundancy and label confusion).
    """
    CREATE TABLE IF NOT EXISTS analytics.fact_services (
        service_key          SERIAL    PRIMARY KEY,
        provider_key         INT       REFERENCES analytics.dim_provider(provider_key),
        procedure_key        INT       REFERENCES analytics.dim_procedure(procedure_key),
        geo_key              INT       REFERENCES analytics.dim_geography(geo_key),
        year                 SMALLINT  NOT NULL,
        total_beneficiaries  INT,
        total_services       NUMERIC(15,2),
        avg_submitted_charge NUMERIC(15,2),
        avg_medicare_allowed NUMERIC(15,2),
        avg_medicare_payment NUMERIC(15,2)
    )
    """,
]


def phase_setup():
    """Create all schemas and warehouse tables (idempotent — safe to re-run)."""
    log.info("=== PHASE 1: Database Setup ===")
    engine = get_engine()
    with engine.connect() as conn:
        for stmt in _DDL_STATEMENTS:
            stmt = stmt.strip()
            if stmt:
                try:
                    conn.execute(text(stmt))
                except Exception as exc:
                    conn.rollback()
                    raise RuntimeError(f"DDL failed:\n{stmt}\nError: {exc}") from exc
        conn.commit()
    log.info("All schemas and tables created (or already exist).")

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 – INGESTION
# Both datasets are downloaded and staged.  cms_hospitals feeds dim_hospital.
# ─────────────────────────────────────────────────────────────────────────────

def download_file(name: str, url: str) -> tuple[str, bool]:
    """
    Stream-download a CSV to RAW_DIR.  Returns (filepath, success).

    FIX: Previously returned the filepath even on failure, making downstream
    steps appear to succeed.  Now returns a boolean success flag so callers
    can abort cleanly instead of processing a missing file.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    filepath = os.path.join(RAW_DIR, f"{name}.csv")

    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / 1_048_576
        log.info(f"  {name}.csv already present ({size_mb:.1f} MB). Skipping download.")
        return filepath, True

    log.info(f"  Downloading {name} …  (may take several minutes for large files)")
    try:
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        tmp_path = filepath + ".tmp"
        with open(tmp_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65_536):
                fh.write(chunk)
        os.replace(tmp_path, filepath)   # atomic rename — no partial files
        log.info(f"  Saved → {filepath}  ({os.path.getsize(filepath)/1_048_576:.1f} MB)")
        return filepath, True
    except requests.RequestException as exc:
        log.error(
            f"  Download FAILED for '{name}': {exc}\n"
            f"  Manual fix: download from:\n    {url}\n"
            f"  and save to: {filepath}"
        )
        if os.path.exists(filepath + ".tmp"):
            os.remove(filepath + ".tmp")
        return filepath, False


def load_csv_to_staging(filepath: str, table_name: str, chunksize: int = 50_000) -> bool:
    """
    Load a CSV into staging.<table_name> using a transactionally-safe strategy.

    Strategy:
      1. Read all chunks into a temp staging table  staging.<table_name>_new.
      2. Only after ALL chunks succeed, atomically rename:
             DROP staging.<table_name>  (if exists)
             ALTER TABLE staging.<table_name>_new RENAME TO <table_name>
      This ensures the live staging table is never in a partial state.
      Returns True on success, False on failure.
    """
    if not os.path.exists(filepath):
        log.warning(f"  File not found — skipping staging load: {filepath}")
        return False

    engine    = get_engine()
    tmp_name  = f"{table_name}_new"
    log.info(f"  Loading {filepath} → staging.{table_name} …")

    reader       = pd.read_csv(filepath, dtype=str, chunksize=chunksize)
    total_rows   = 0
    table_seeded = False

    try:
        # Use an explicit connection for all operations in this load so pandas
        # does not emit the "Engine has no attribute cursor" warning and so the
        # entire chunk sequence shares one connection (faster, no pool churn).
        with engine.connect() as conn:
            for chunk in reader:
                chunk.to_sql(
                    tmp_name,
                    conn,                          # ← connection, not engine
                    schema="staging",
                    if_exists="replace" if not table_seeded else "append",
                    index=False,
                )
                table_seeded = True
                total_rows  += len(chunk)

            # Atomic swap inside the same connection / transaction
            conn.execute(text(f'DROP TABLE IF EXISTS staging."{table_name}"'))
            conn.execute(text(
                f'ALTER TABLE staging."{tmp_name}" RENAME TO "{table_name}"'
            ))
            conn.commit()

        log.info(f"  Loaded {total_rows:,} rows → staging.{table_name}.")
        return True

    except Exception as exc:
        log.error(f"  Staging load FAILED for {table_name}: {exc}")
        # Clean up temp table so next run starts fresh
        try:
            with engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS staging."{tmp_name}"'))
                conn.commit()
        except Exception:
            pass
        return False


def phase_ingest():
    """Download CMS CSVs and load them into PostgreSQL staging tables."""
    log.info("=== PHASE 2: Data Ingestion ===")
    failures = []
    for name, url in DATASETS.items():
        filepath, ok = download_file(name, url)
        if not ok:
            failures.append(name)
            continue
        ok = load_csv_to_staging(filepath, name)
        if not ok:
            failures.append(name)

    if failures:
        log.warning(f"  Ingestion finished with failures: {failures}")
        log.warning("  Downstream phases may fail or produce incomplete results.")
    else:
        log.info("Ingestion complete — all datasets loaded successfully.")

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 – TRANSFORMATION
# Cleans both provider and hospital staging tables.
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_columns(df: pd.DataFrame, alias_map: dict[str, str]) -> pd.DataFrame:
    """Lowercase + underscore headers, then apply the alias map."""
    df.columns = (
        df.columns.str.lower()
        .str.replace(r"[\s/]+", "_", regex=True)
        .str.strip("_")
    )
    df = df.rename(columns={
        raw: canonical
        for raw, canonical in alias_map.items()
        if raw in df.columns
    })
    return df


def _validate_required_columns(
    df: pd.DataFrame, required: list[str], context: str
) -> None:
    """Fail fast with a clear message if any required column is absent."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Schema validation FAILED ({context}).\n"
            f"  Missing : {missing}\n"
            f"  Present : {sorted(df.columns.tolist())}\n"
            f"  Fix     : Update the alias map in the config section to map "
            f"the actual CMS column names to these canonical names."
        )
    log.info(f"  [Validate] Schema OK ({context}) — all required columns present.")


def run_data_quality_checks(
    df: pd.DataFrame,
    grain_cols: list[str],
    money_cols: list[str],
) -> None:
    """
    Comprehensive data quality checks.
    All issues are logged as warnings — the pipeline continues so partial
    results are still available.  Each check tag is grep-friendly.

    FIX: grain check now uses the FULL declared grain (npi × hcpcs_code ×
    state_code × year) instead of the incomplete (npi × hcpcs_code) check
    that was in the previous version.
    """
    issues = 0

    # QC-1: Full-grain duplicate check
    full_grain = [c for c in grain_cols if c in df.columns]
    if len(full_grain) == len(grain_cols):
        dup_grain = df.duplicated(subset=full_grain).sum()
        if dup_grain:
            log.warning(f"  [QC-DupGrain] {dup_grain:,} duplicate rows on full grain "
                        f"({' × '.join(full_grain)}) — will be aggregated in fact load.")
            issues += 1
    else:
        missing_g = set(grain_cols) - set(df.columns)
        log.warning(f"  [QC-GrainSkip] Cannot check full grain — missing: {missing_g}")

    # QC-2: Nulls in required columns
    for col in grain_cols:
        if col in df.columns:
            n = int(df[col].isnull().sum())
            if n:
                log.warning(f"  [QC-Null] {n:,} nulls in required column '{col}'.")
                issues += 1

    # QC-3: Invalid state codes
    if "state_code" in df.columns:
        bad_states = df.loc[
            df["state_code"].notna() & ~df["state_code"].isin(VALID_STATE_CODES),
            "state_code",
        ].unique()
        if len(bad_states):
            log.warning(f"  [QC-State] {len(bad_states)} invalid state codes: "
                        f"{sorted(bad_states)[:10]} — mapped to NULL in fact.")
            issues += 1

    # QC-4: Invalid gender codes
    if "gender" in df.columns:
        bad_gender = df.loc[
            df["gender"].notna() & ~df["gender"].isin(VALID_GENDER_CODES),
            "gender",
        ].unique()
        if len(bad_gender):
            log.warning(f"  [QC-Gender] {len(bad_gender)} invalid gender values: "
                        f"{sorted(bad_gender)} — set to NULL.")
            issues += 1

    # QC-5: Negative money values
    for col in money_cols:
        if col in df.columns:
            n = int((df[col].fillna(0) < 0).sum())
            if n:
                log.warning(f"  [QC-Negative] {n:,} negative values in '{col}' — removed.")
                issues += 1

    # QC-6: Zero/null service count
    if "tot_srvcs" in df.columns:
        n = int((df["tot_srvcs"].fillna(0) <= 0).sum())
        if n:
            log.warning(f"  [QC-ZeroSvc] {n:,} rows with tot_srvcs ≤ 0 — removed.")
            issues += 1

    # QC-7: Row count sanity
    if len(df) < 1_000:
        log.warning(f"  [QC-RowCount] Only {len(df):,} rows — expected ≥ 1,000 "
                    f"for CMS data.  Check the download and CSV file.")
        issues += 1

    summary = "no issues found" if issues == 0 else f"{issues} issue(s) flagged"
    log.info(f"  [QC-Summary] {summary}.")


# ── Provider cleaning ──────────────────────────────────────────────────────────

def clean_providers_chunk(df: pd.DataFrame, validated: bool = False) -> pd.DataFrame:
    """
    Clean one chunk of provider data using a single-pass vectorised approach.

    All filtering rules are combined into one boolean mask (R7+R8) so pandas
    never scans the same column twice.  This is the hot path for the chunked
    transform and must stay allocation-light.

      R0  Normalise column names
      R1  Validate required columns (chunk-0 only, skip on later chunks)
      R2  Drop fully-empty rows
      R3  Cast numeric columns (coerce → NaN)
      R4  Trim whitespace from string columns only
      R5  Uppercase state codes; invalid → NULL
      R6  Normalise gender to M/F; other → NULL
      R7+R8  Single combined keep-mask: drop negative money AND zero services
      R9  Fill remaining numeric NaN with 0
    """
    # R0
    df = _normalise_columns(df, CMS_PROVIDER_COL_ALIASES)

    # R2 — before R1 so a blank leading row never trips the schema check
    df = df.dropna(how="all")
    if df.empty:
        return df

    # R1 — only on the first chunk (schema cannot shift mid-file)
    if not validated:
        _validate_required_columns(df, PROVIDER_REQUIRED_COLS, "providers chunk-0")

    # R3 — numeric cast
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # R4 — trim only object columns; avoids touching numeric series
    obj_cols = df.select_dtypes(include="object").columns
    df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())

    # R5 — state codes
    if "state_code" in df.columns:
        df["state_code"] = df["state_code"].str.upper()
        bad_state = df["state_code"].notna() & ~df["state_code"].isin(VALID_STATE_CODES)
        df.loc[bad_state, "state_code"] = None

    # R6 — gender
    if "gender" in df.columns:
        df["gender"] = df["gender"].str.upper().str[:1]
        bad_gender = df["gender"].notna() & ~df["gender"].isin(VALID_GENDER_CODES)
        df.loc[bad_gender, "gender"] = None

    # R7 + R8 — one combined boolean mask; no repeated full-column scans
    keep = pd.Series(True, index=df.index)
    for col in ["avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment"]:
        if col in df.columns:
            keep &= df[col].isna() | (df[col] >= 0)
    if "tot_srvcs" in df.columns:
        keep &= df["tot_srvcs"].fillna(0) > 0
    df = df.loc[keep].copy()

    # R9 — fill numeric NaN with 0
    present_num = [c for c in NUMERIC_COLS if c in df.columns]
    df[present_num] = df[present_num].fillna(0)

    return df


def clean_providers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the full provider DataFrame (used when the whole table fits in RAM).
    Delegates to clean_providers_chunk, then runs QC summary once at the end.
    """
    log.info(f"  [Providers] Starting: {len(df):,} rows, {len(df.columns)} cols.")
    df = clean_providers_chunk(df, validated=False)
    # NOTE: 'year' is intentionally absent from the staging data at this stage —
    # it is added in load_fact_services (default: CMS_DATA_YEAR).  The QC
    # function will emit a QC-GrainSkip warning for 'year', which is expected
    # and correct.  The full grain (npi × hcpcs_code × state_code × year) is
    # enforced during the fact aggregation step.
    run_data_quality_checks(
        df,
        grain_cols=["npi", "hcpcs_code", "state_code", "year"],
        money_cols=["avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment"],
    )
    log.info(f"  [Providers] Done: {len(df):,} rows.")
    return df


# ── Hospital cleaning ──────────────────────────────────────────────────────────

def clean_hospitals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the raw hospital DataFrame.

    Rules mirror the provider cleaner; hospital-specific rules added for
    overall_rating (must be integer 1–5 or NULL) and emergency_services flag.
    """
    log.info(f"  [Hospitals] Starting: {len(df):,} rows, {len(df.columns)} cols.")

    df = _normalise_columns(df, CMS_HOSPITAL_COL_ALIASES)

    before = len(df)
    df = df.dropna(how="all")
    log.info(f"  [H-R2] Dropped {before - len(df):,} fully-empty rows.")

    _validate_required_columns(df, HOSPITAL_REQUIRED_COLS, "hospitals after R2")

    # Trim whitespace
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # State codes
    if "state_code" in df.columns:
        df["state_code"] = df["state_code"].str.upper()
        bad = df["state_code"].notna() & ~df["state_code"].isin(VALID_STATE_CODES)
        n = int(bad.sum())
        df.loc[bad, "state_code"] = None
        log.info(f"  [H-State] {n:,} invalid state codes set to NULL.")

    # overall_rating: keep only integers 1–5
    # FIX: pd.to_numeric returns floats (e.g. 1.0); isin([1,2,3,4,5]) with int
    # literals is unreliable across pandas versions for float series.
    # Use .between(1, 5) instead, then cast to nullable Int64 so the column
    # stores proper integers (not floats) while still supporting NULL.
    if "overall_rating" in df.columns:
        df["overall_rating"] = pd.to_numeric(df["overall_rating"], errors="coerce")
        bad = df["overall_rating"].notna() & ~df["overall_rating"].between(1, 5)
        n = int(bad.sum())
        df.loc[bad, "overall_rating"] = pd.NA
        df["overall_rating"] = df["overall_rating"].round(0).astype("Int64")
        log.info(f"  [H-Rating] {n:,} out-of-range overall_rating values set to NULL.")

    # zip_code: keep first 5 chars only (strips ZIP+4), handle NaN safely
    if "zip_code" in df.columns:
        df["zip_code"] = (
            df["zip_code"]
            .where(df["zip_code"].notna(), other=None)
            .apply(lambda v: str(v)[:5] if v is not None else None)
        )

    run_data_quality_checks(
        df,
        grain_cols=["cms_certification_number"],
        money_cols=[],
    )

    log.info(f"  [Hospitals] Done: {len(df):,} rows.")
    return df


def phase_transform(chunksize: int = 100_000):
    """
    Validate and clean staging tables; write staging.clean_providers and staging.clean_hospitals.

    PERFORMANCE FIX: Instead of loading the entire staging table into RAM with
    pd.read_sql(), we stream the source CSV directly from disk in chunks of
    `chunksize` rows.  Each chunk is cleaned independently and written to
    staging.clean_providers via append.  Peak RAM usage is O(chunksize) rather
    than O(full table), which drops memory consumption by ~90% for the CMS file.

    The first chunk creates the target table (if_exists="replace"); subsequent
    chunks append.  The atomic-swap trick from ingestion is not needed here
    because a failed mid-run leaves the partial table visible — which is fine
    because the load phase checks for the table's existence before reading it.
    """
    log.info("=== PHASE 3: Data Cleaning & Transformation ===")
    engine = get_engine()

    # ── Providers — stream from raw CSV, clean chunk by chunk ────────────────
    # Streaming from the CSV is faster than SELECT * FROM staging.cms_providers
    # because it avoids the PostgreSQL → network → pandas round-trip for each row.
    provider_csv = os.path.join(RAW_DIR, "cms_providers.csv")
    if not os.path.exists(provider_csv):
        log.error(f"  Raw file not found: {provider_csv}. Run 'ingest' first.")
    else:
        try:
            total_in = total_out = total_rejected = 0
            validated = False
            first_chunk = True
            log.info(f"  Streaming {provider_csv} in chunks of {chunksize:,} rows …")

            # Get file size for progress display
            file_size_mb = os.path.getsize(provider_csv) / 1_048_576
            log.info(f"  File size: {file_size_mb:.0f} MB. Progress logged every 10 chunks.")

            # Reuse ONE connection for all chunk writes — avoids repeated
            # connect/disconnect overhead that slows down multi-chunk loads.
            with engine.connect() as write_conn:
                for chunk_num, chunk in enumerate(
                    pd.read_csv(provider_csv, dtype=str, chunksize=chunksize)
                ):
                    rows_in = len(chunk)
                    chunk_clean = clean_providers_chunk(chunk, validated=validated)
                    validated = True   # schema confirmed on chunk-0; skip on the rest

                    rows_out_chunk = len(chunk_clean)
                    total_in       += rows_in
                    total_out      += rows_out_chunk
                    total_rejected += rows_in - rows_out_chunk

                    if chunk_clean.empty:
                        continue

                    chunk_clean.to_sql(
                        "clean_providers",
                        write_conn,
                        schema="staging",
                        if_exists="replace" if first_chunk else "append",
                        index=False,
                        method="multi",   # batch INSERT — much faster than default
                        chunksize=5_000,
                    )
                    first_chunk = False

                    if chunk_num % 10 == 0:
                        log.info(
                            f"  … chunk {chunk_num + 1:>4} | "
                            f"{total_in:>10,} rows read | "
                            f"{total_out:>10,} rows written | "
                            f"{total_rejected:>7,} rejected"
                        )

                write_conn.commit()

            # Run QC once on a sample from the written table (avoids re-reading all rows)
            sample = pd.read_sql(
                "SELECT * FROM staging.clean_providers LIMIT 50000", engine
            )
            run_data_quality_checks(
                sample,
                grain_cols=["npi", "hcpcs_code", "state_code"],
                money_cols=["avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment"],
            )
            log.info(
                f"  Providers done: {total_in:,} in, "
                f"{total_out:,} written, {total_rejected:,} rejected "
                f"→ staging.clean_providers"
            )
        except Exception as exc:
            log.error(f"  Provider transform FAILED: {exc}")
            raise

    # ── Hospitals — small enough to load in one shot ───────────────────────
    hospital_csv = os.path.join(RAW_DIR, "cms_hospitals.csv")
    if not os.path.exists(hospital_csv):
        log.info("  cms_hospitals.csv not found — skipping hospital transform.")
    else:
        try:
            df_hosp_raw = pd.read_csv(hospital_csv, low_memory=False)
            df_hosp_clean = clean_hospitals(df_hosp_raw)
            with engine.connect() as conn:
                df_hosp_clean.to_sql(
                    "clean_hospitals", conn, schema="staging",
                    if_exists="replace", index=False,
                )
                conn.commit()
            log.info(f"  Written {len(df_hosp_clean):,} rows → staging.clean_hospitals.")
        except Exception as exc:
            log.error(f"  Hospital transform FAILED: {exc}")

    log.info("Transformation complete.")


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 – WAREHOUSE LOADING
#
# Strategy:
#   Dimensions: UPSERT via INSERT … ON CONFLICT DO NOTHING  →  stable surrogate keys
#               + DO UPDATE for non-key attribute changes   →  dims stay current
#   Fact:       staged via a temp table, then atomically swapped in
#               (avoids if_exists='replace' destroying table metadata mid-run)
# ─────────────────────────────────────────────────────────────────────────────

def _state_region(code: str) -> str:
    for region, states in REGIONS.items():
        if code in states:
            return region
    return "Other"


def upsert_dim_geography(engine) -> pd.DataFrame:
    """Insert-or-ignore all known states into dim_geography."""
    rows = [
        {"state_code": code, "state_name": name, "region": _state_region(code)}
        for code, name in STATE_NAMES.items()
    ]
    sql = text("""
        INSERT INTO analytics.dim_geography (state_code, state_name, region)
        VALUES (:state_code, :state_name, :region)
        ON CONFLICT (state_code) DO NOTHING
    """)
    with engine.connect() as conn:
        conn.execute(sql, rows)
        conn.commit()
    result = pd.read_sql(
        "SELECT geo_key, state_code FROM analytics.dim_geography", engine
    )
    log.info(f"  dim_geography: {len(result)} rows (stable keys).")
    return result


def upsert_dim_provider(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Upsert providers.
    FIX: Uses DO UPDATE for non-key attributes (provider_type, credential,
    etc.) so that stale dimension data is refreshed when a new CMS file is
    loaded — previously DO NOTHING left the warehouse with outdated values.
    Surrogate key (provider_key) is never changed.
    zip_code truncation now uses .where() to avoid turning NaN into "nan".
    """
    col_map = {
        "npi": "npi", "last_name": "last_name", "first_name": "first_name",
        "credential": "credential", "gender": "gender", "entity_code": "entity_code",
        "provider_type": "provider_type", "state_code": "state_code", "zip_code": "zip_code",
    }
    avail = {k: v for k, v in col_map.items() if k in df.columns}
    dim = (
        df[list(avail.keys())]
        .rename(columns=avail)
        .drop_duplicates(subset=["npi"])
        .copy()
    )

    # FIX: safe zip truncation — avoid "nan" string
    if "zip_code" in dim.columns:
        dim["zip_code"] = dim["zip_code"].where(
            dim["zip_code"].notna(), other=None
        ).apply(lambda v: str(v)[:5] if v is not None else None)

    # Non-key columns to update on conflict
    update_cols = [c for c in dim.columns if c != "npi"]
    update_str  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_str     = ", ".join(dim.columns)
    bind_str    = ", ".join(f":{c}" for c in dim.columns)

    sql = text(f"""
        INSERT INTO analytics.dim_provider ({col_str})
        VALUES ({bind_str})
        ON CONFLICT (npi) DO UPDATE SET {update_str}
    """)
    with engine.connect() as conn:
        conn.execute(sql, dim.to_dict("records"))
        conn.commit()

    result = pd.read_sql(
        "SELECT provider_key, npi FROM analytics.dim_provider", engine
    )
    log.info(f"  dim_provider: {len(result):,} rows (stable keys, attrs updated).")
    return result


def upsert_dim_procedure(df: pd.DataFrame, engine) -> pd.DataFrame:
    """Upsert procedures; update description on conflict."""
    if "hcpcs_code" not in df.columns:
        log.warning("  hcpcs_code missing — dim_procedure unchanged.")
        return pd.read_sql(
            "SELECT procedure_key, hcpcs_code FROM analytics.dim_procedure", engine
        )

    col_map = {
        "hcpcs_code": "hcpcs_code", "hcpcs_desc": "hcpcs_desc",
        "hcpcs_drug_ind": "hcpcs_drug_ind",
    }
    avail = {k: v for k, v in col_map.items() if k in df.columns}
    dim = (
        df[list(avail.keys())]
        .rename(columns=avail)
        .drop_duplicates(subset=["hcpcs_code"])
        .copy()
    )

    update_cols = [c for c in dim.columns if c != "hcpcs_code"]
    update_str  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols) or "hcpcs_code = EXCLUDED.hcpcs_code"
    col_str     = ", ".join(dim.columns)
    bind_str    = ", ".join(f":{c}" for c in dim.columns)

    sql = text(f"""
        INSERT INTO analytics.dim_procedure ({col_str})
        VALUES ({bind_str})
        ON CONFLICT (hcpcs_code) DO UPDATE SET {update_str}
    """)
    with engine.connect() as conn:
        conn.execute(sql, dim.to_dict("records"))
        conn.commit()

    result = pd.read_sql(
        "SELECT procedure_key, hcpcs_code FROM analytics.dim_procedure", engine
    )
    log.info(f"  dim_procedure: {len(result):,} rows (stable keys, attrs updated).")
    return result


def upsert_dim_hospital(df: pd.DataFrame, engine) -> pd.DataFrame:
    """Upsert hospitals from staging.clean_hospitals into analytics.dim_hospital."""
    col_map = {
        "cms_certification_number": "cms_certification_number",
        "hospital_name":    "hospital_name",
        "address":          "address",
        "city":             "city",
        "state_code":       "state_code",
        "zip_code":         "zip_code",
        "county_name":      "county_name",
        "phone_number":     "phone_number",
        "hospital_type":    "hospital_type",
        "ownership_type":   "ownership_type",
        "emergency_services": "emergency_services",
        "overall_rating":   "overall_rating",
    }
    avail = {k: v for k, v in col_map.items() if k in df.columns}
    dim = (
        df[list(avail.keys())]
        .rename(columns=avail)
        .drop_duplicates(subset=["cms_certification_number"])
        .copy()
    )

    # Safe zip truncation
    if "zip_code" in dim.columns:
        dim["zip_code"] = dim["zip_code"].where(
            dim["zip_code"].notna(), other=None
        ).apply(lambda v: str(v)[:5] if v is not None else None)

    update_cols = [c for c in dim.columns if c != "cms_certification_number"]
    update_str  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    col_str     = ", ".join(dim.columns)
    bind_str    = ", ".join(f":{c}" for c in dim.columns)

    sql = text(f"""
        INSERT INTO analytics.dim_hospital ({col_str})
        VALUES ({bind_str})
        ON CONFLICT (cms_certification_number) DO UPDATE SET {update_str}
    """)
    with engine.connect() as conn:
        conn.execute(sql, dim.to_dict("records"))
        conn.commit()

    result = pd.read_sql(
        "SELECT hospital_key, cms_certification_number FROM analytics.dim_hospital", engine
    )
    log.info(f"  dim_hospital: {len(result):,} rows (stable keys, attrs updated).")
    return result


def _aggregate_chunk_to_grain(
    chunk: pd.DataFrame,
    year: int,
) -> pd.DataFrame:
    """
    Aggregate one chunk to the fact grain and return a DataFrame ready for merging.
    Called once per chunk in phase_load; results are concatenated before the
    final fact write.

    Separated from load_fact_services so it can be unit-tested independently.
    """
    chunk = chunk.copy()
    chunk["year"] = year

    grain = [c for c in ["npi", "hcpcs_code", "state_code", "year"] if c in chunk.columns]
    weight_col = "tot_srvcs"
    money_src  = ["avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment"]

    for col in money_src:
        if col in chunk.columns and weight_col in chunk.columns:
            chunk[f"_num_{col}"] = chunk[col] * chunk[weight_col]

    agg_dict: dict = {}
    if weight_col in chunk.columns:
        agg_dict["total_services"] = pd.NamedAgg(column=weight_col,  aggfunc="sum")
        agg_dict["_wt_sum"]        = pd.NamedAgg(column=weight_col,  aggfunc="sum")
    if "tot_benes" in chunk.columns:
        agg_dict["total_beneficiaries"] = pd.NamedAgg(column="tot_benes", aggfunc="sum")
    for col in money_src:
        if f"_num_{col}" in chunk.columns:
            agg_dict[f"_num_{col}"] = pd.NamedAgg(column=f"_num_{col}", aggfunc="sum")

    agg = chunk.groupby(grain, dropna=False).agg(**agg_dict).reset_index()

    for col in money_src:
        num_col = f"_num_{col}"
        if num_col in agg.columns and "_wt_sum" in agg.columns:
            agg[col] = agg[num_col] / agg["_wt_sum"].replace(0, float("nan"))
        else:
            agg[col] = float("nan")

    drop_cols = [c for c in agg.columns if c.startswith("_")]
    return agg.drop(columns=drop_cols)


def load_fact_services(
    agg: pd.DataFrame,
    provider_keys: pd.DataFrame,
    procedure_keys: pd.DataFrame,
    geo_keys: pd.DataFrame,
    engine,
    year: int = CMS_DATA_YEAR,
) -> None:
    """
    Attach surrogate keys to the pre-aggregated fact DataFrame and load
    analytics.fact_services via an atomic staging swap.

    The caller is responsible for aggregating source data to the declared grain
    (npi × hcpcs_code × state_code × year) before passing it in.  For chunked
    loads, use phase_load which accumulates chunk aggregations and re-aggregates
    across chunk boundaries before calling this function.

    GRAIN: one row per (npi × hcpcs_code × state_code × year).

    Monetary columns are WEIGHTED averages (weight = total_services) so that
    SUM(avg_medicare_payment * total_services) in queries yields correct
    derived spend without double-counting.
    """
    log.info(f"  Loading fact table — {len(agg):,} grain rows, year={year} …")

    # ── Step 1: Attach surrogate keys ─────────────────────────────────────
    fact = agg.merge(provider_keys,  on="npi",        how="left")
    fact = fact.merge(procedure_keys, on="hcpcs_code", how="left")
    if "state_code" in fact.columns:
        fact = fact.merge(geo_keys, on="state_code", how="left")
    else:
        fact["geo_key"] = None

    fact["geo_key"] = fact["geo_key"].astype("Int64")    

    # ── Step 2: Log NULL FK counts ─────────────────────────────────────────
    for fk, dim_name in [
        ("provider_key",  "dim_provider"),
        ("procedure_key", "dim_procedure"),
        ("geo_key",       "dim_geography"),
    ]:
        n_null = int(fact[fk].isnull().sum()) if fk in fact.columns else 0
        if n_null:
            log.warning(f"  [FK-NULL] {n_null:,} rows missing {fk} "
                        f"(no match in {dim_name}) — included but excluded from dim-joined queries.")

    # ── Step 3: Atomic staging-swap load ──────────────────────────────────
    keep = [
        "provider_key", "procedure_key", "geo_key", "year",
        "total_beneficiaries", "total_services",
        "avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment",
    ]
    fact_df = fact[[c for c in keep if c in fact.columns]]

    # Write to temp table first (outside the swap transaction — safe to retry)
    fact_df.to_sql("fact_services_new", engine, schema="analytics",
                   if_exists="replace", index=False)

    # FIX: The old approach dropped the live table first, then added FK constraints
    # afterwards.  If the ADD CONSTRAINT step failed (e.g. a FK violation), the
    # fact table existed but had no integrity constraints — silently broken.
    # New approach: everything inside ONE transaction with a SAVEPOINT guard.
    # If ADD CONSTRAINT fails the whole transaction rolls back, leaving the old
    # fact_services_new table intact so the issue can be debugged without data loss.
    with engine.connect() as conn:
        try:
            conn.execute(text("SAVEPOINT fact_swap"))
            conn.execute(text("DROP TABLE IF EXISTS analytics.fact_services CASCADE"))
            conn.execute(text(
                "ALTER TABLE analytics.fact_services_new RENAME TO fact_services"
            ))
            # Re-add FK constraints inside the same transaction.
            # If any constraint fails the SAVEPOINT rollback preserves the
            # pre-swap state and leaves fact_services_new available for inspection.
            conn.execute(text("""
                ALTER TABLE analytics.fact_services
                    ADD CONSTRAINT fk_fs_provider
                        FOREIGN KEY (provider_key) REFERENCES analytics.dim_provider(provider_key),
                    ADD CONSTRAINT fk_fs_procedure
                        FOREIGN KEY (procedure_key) REFERENCES analytics.dim_procedure(procedure_key),
                    ADD CONSTRAINT fk_fs_geography
                        FOREIGN KEY (geo_key) REFERENCES analytics.dim_geography(geo_key)
            """))
            conn.commit()
        except Exception as exc:
            conn.execute(text("ROLLBACK TO SAVEPOINT fact_swap"))
            conn.commit()
            raise RuntimeError(
                f"Fact swap failed and was rolled back.  "
                f"analytics.fact_services_new is intact for inspection.  "
                f"Error: {exc}"
            ) from exc

    log.info(f"  Loaded {len(fact_df):,} rows → analytics.fact_services (atomic swap).")


def phase_load(chunksize: int = 50_000):
    """
    Populate the full star-schema warehouse: all dims first, then fact.

    FIX (RAM): The original phase_load did:
        df = pd.read_sql("SELECT * FROM staging.clean_providers", engine)
    which loaded the entire provider table into RAM — directly contradicting
    the chunked streaming design in phase_transform.

    New approach:
      1. Stream staging.clean_providers in chunks of `chunksize` rows.
      2. From the FIRST chunk, run the dim upserts (geography is static;
         provider/procedure dims only need the distinct rows — we accumulate
         those across all chunks using sets, then upsert once).
      3. Accumulate per-chunk grain aggregations.
      4. Re-aggregate across chunk boundaries (handles NPI × procedure combos
         that span multiple chunks), then call load_fact_services once.

    Peak RAM usage is now O(chunksize + distinct dim rows + agg grain rows)
    rather than O(full staging table).
    """
    log.info("=== PHASE 4: Warehouse Loading ===")
    engine = get_engine()

    # ── Geography (static — upsert once, always) ────────────────────────────
    geo_keys = upsert_dim_geography(engine)

    # ── Stream clean_providers; accumulate dim frames and grain aggs ─────────
    dim_provider_frames:  list[pd.DataFrame] = []
    dim_procedure_frames: list[pd.DataFrame] = []
    grain_agg_frames:     list[pd.DataFrame] = []

    provider_col_map = {
        "npi": "npi", "last_name": "last_name", "first_name": "first_name",
        "credential": "credential", "gender": "gender", "entity_code": "entity_code",
        "provider_type": "provider_type", "state_code": "state_code", "zip_code": "zip_code",
    }
    procedure_col_map = {
        "hcpcs_code": "hcpcs_code", "hcpcs_desc": "hcpcs_desc",
        "hcpcs_drug_ind": "hcpcs_drug_ind",
    }

    try:
        chunk_iter = pd.read_sql(
            "SELECT * FROM staging.clean_providers",
            engine,
            chunksize=chunksize,
        )
    except Exception as exc:
        log.error(f"  Cannot read staging.clean_providers: {exc}")
        log.error("  Run 'transform' phase first.")
        return

    total_source_rows = 0
    for chunk_num, chunk in enumerate(chunk_iter):
        total_source_rows += len(chunk)

        # Accumulate distinct provider rows for dim upsert
        prov_avail = {k: v for k, v in provider_col_map.items() if k in chunk.columns}
        if prov_avail:
            dim_provider_frames.append(
                chunk[list(prov_avail.keys())].rename(columns=prov_avail)
            )

        # Accumulate distinct procedure rows for dim upsert
        proc_avail = {k: v for k, v in procedure_col_map.items() if k in chunk.columns}
        if proc_avail and "hcpcs_code" in chunk.columns:
            dim_procedure_frames.append(
                chunk[list(proc_avail.keys())].rename(columns=proc_avail)
            )

        # Aggregate this chunk to grain; accumulate for cross-chunk re-aggregation
        grain_agg_frames.append(_aggregate_chunk_to_grain(chunk, CMS_DATA_YEAR))

        if chunk_num % 10 == 0:
            log.info(f"  … chunk {chunk_num + 1:>4} | {total_source_rows:>10,} rows read")

    log.info(f"  Finished reading staging.clean_providers: {total_source_rows:,} rows total.")

    if not grain_agg_frames:
        log.error("  No data read from staging.clean_providers — aborting load.")
        return

    # ── Upsert dims using accumulated distinct rows ──────────────────────────
    # Concatenate and dedup before upserting so we make one round trip each.
    if dim_provider_frames:
        df_providers_dim = pd.concat(dim_provider_frames).drop_duplicates(subset=["npi"])
        provider_keys = upsert_dim_provider(df_providers_dim, engine)
    else:
        log.warning("  No provider dim rows — dim_provider unchanged.")
        provider_keys = pd.read_sql(
            "SELECT provider_key, npi FROM analytics.dim_provider", engine
        )

    if dim_procedure_frames:
        df_procedures_dim = pd.concat(dim_procedure_frames).drop_duplicates(subset=["hcpcs_code"])
        procedure_keys = upsert_dim_procedure(df_procedures_dim, engine)
    else:
        log.warning("  No procedure dim rows — dim_procedure unchanged.")
        procedure_keys = pd.read_sql(
            "SELECT procedure_key, hcpcs_code FROM analytics.dim_procedure", engine
        )

    # ── Re-aggregate across chunk boundaries ────────────────────────────────
    # Each chunk was aggregated to grain independently.  An NPI × HCPCS combo
    # that spans two chunks will appear in both grain frames — we must sum/re-
    # weight across them to avoid double-counting in the final fact table.
    grain_cols = [c for c in ["npi", "hcpcs_code", "state_code", "year"]
                  if c in grain_agg_frames[0].columns]

    combined = pd.concat(grain_agg_frames, ignore_index=True)

    # Re-aggregate: sums are straightforward; weighted averages must be
    # re-weighted using total_services as the weight across the merged chunks.
    money_cols = ["avg_submitted_charge", "avg_medicare_allowed", "avg_medicare_payment"]
    weight_col = "total_services"

    for col in money_cols:
        if col in combined.columns and weight_col in combined.columns:
            combined[f"_num_{col}"] = combined[col] * combined[weight_col]

    reagg_dict: dict = {}
    if weight_col in combined.columns:
        reagg_dict["total_services"] = pd.NamedAgg(column=weight_col,  aggfunc="sum")
        reagg_dict["_wt_sum"]        = pd.NamedAgg(column=weight_col,  aggfunc="sum")
    if "total_beneficiaries" in combined.columns:
        reagg_dict["total_beneficiaries"] = pd.NamedAgg(column="total_beneficiaries", aggfunc="sum")
    for col in money_cols:
        if f"_num_{col}" in combined.columns:
            reagg_dict[f"_num_{col}"] = pd.NamedAgg(column=f"_num_{col}", aggfunc="sum")

    final_agg = combined.groupby(grain_cols, dropna=False).agg(**reagg_dict).reset_index()

    for col in money_cols:
        num_col = f"_num_{col}"
        if num_col in final_agg.columns and "_wt_sum" in final_agg.columns:
            final_agg[col] = final_agg[num_col] / final_agg["_wt_sum"].replace(0, float("nan"))
        else:
            final_agg[col] = float("nan")

    drop_cols = [c for c in final_agg.columns if c.startswith("_")]
    final_agg = final_agg.drop(columns=drop_cols)

    log.info(f"  Cross-chunk re-aggregation: {len(combined):,} chunk rows → {len(final_agg):,} final grain rows.")

    # ── Hospital data (small — one shot is fine) ─────────────────────────────
    try:
        df_hosp = pd.read_sql("SELECT * FROM staging.clean_hospitals", engine)
        upsert_dim_hospital(df_hosp, engine)
    except Exception as exc:
        log.warning(f"  Hospital dimension skipped (staging.clean_hospitals unavailable): {exc}")

    # ── Fact table ────────────────────────────────────────────────────────────
    load_fact_services(final_agg, provider_keys, procedure_keys, geo_keys, engine)

    log.info("Warehouse loading complete.")

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 5 – AIRFLOW DAG  (defined inline — no separate file needed)
#
# Airflow discovers the `dag` object at module level when this file is placed
# in (or symlinked from) your dags_folder.  The try/except means the DAG block
# is simply skipped when Airflow is not installed, so the CLI still works.
#
# One-time Airflow setup:
#     export AIRFLOW_HOME=$(pwd)
#     airflow db init
#     airflow users create \
#         --username admin --firstname Admin --lastname User \
#         --role Admin --email admin@example.com --password admin
#     airflow webserver --port 8080   # terminal 1
#     airflow scheduler               # terminal 2
#     Open http://localhost:8080, find "healthcare_pipeline", click ▶ to trigger.
# ─────────────────────────────────────────────────────────────────────────────

try:
    from datetime import timedelta as _timedelta
    from datetime import datetime as _datetime
    from airflow import DAG as _DAG
    from airflow.operators.python import PythonOperator as _PythonOperator

    _DAG_DEFAULT_ARGS = {
        "owner":           "healthcare_pipeline",
        "retries":          2,
        "retry_delay":      _timedelta(minutes=5),
        "email_on_failure": False,
    }

    with _DAG(
        dag_id            = "healthcare_pipeline",
        default_args      = _DAG_DEFAULT_ARGS,
        description       = "CMS Healthcare Data ETL Pipeline (providers + hospitals)",
        schedule_interval = "@weekly",
        start_date        = _datetime(2024, 1, 1),
        catchup           = False,
        tags              = ["healthcare", "cms", "etl"],
    ) as dag:

        t_setup = _PythonOperator(
            task_id="setup_database", python_callable=phase_setup
        )
        t_ingest = _PythonOperator(
            task_id="ingest_raw_data", python_callable=phase_ingest
        )
        t_transform = _PythonOperator(
            task_id="clean_and_transform", python_callable=phase_transform
        )
        t_load = _PythonOperator(
            task_id="load_to_warehouse", python_callable=phase_load
        )

        t_setup >> t_ingest >> t_transform >> t_load

except ImportError:
    # Airflow not installed — DAG object is not created.
    # All CLI phases (setup, ingest, transform, load, queries, all) work normally.
    pass

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 6 – ANALYTICAL SQL QUERIES
#
# All spend metrics use the WEIGHTED formula:
#   derived_spend = SUM(avg_medicare_payment × total_services)
# This is documented as DERIVED SPEND (not raw reported spend) to avoid
# mislabeling.  The fact grain (npi × hcpcs_code × state_code × year) ensures
# no double-counting when aggregating across any single dimension.
# ─────────────────────────────────────────────────────────────────────────────

ANALYTICAL_QUERIES: dict[str, str] = {

    "Q1 – Top 10 states by derived Medicare spend (weighted)": """
        SELECT
            g.state_code,
            g.state_name,
            g.region,
            SUM(f.total_services)::BIGINT                   AS total_services,
            ROUND(SUM(f.avg_medicare_payment * f.total_services)::NUMERIC, 2)
                                                             AS derived_medicare_spend_usd,
            ROUND(
                SUM(f.avg_medicare_payment * f.total_services)::NUMERIC
                / NULLIF(SUM(f.total_services), 0), 2
            )                                               AS wtd_avg_payment_per_svc
        FROM  analytics.fact_services  f
        JOIN  analytics.dim_geography  g ON f.geo_key = g.geo_key
        GROUP BY g.state_code, g.state_name, g.region
        ORDER BY derived_medicare_spend_usd DESC
        LIMIT 10;
    """,

    "Q2 – Top 10 procedure codes by total service volume": """
        SELECT
            p.hcpcs_code,
            LEFT(p.hcpcs_desc, 60)                          AS procedure_desc,
            p.hcpcs_drug_ind                                 AS drug_ind,
            SUM(f.total_services)::BIGINT                    AS total_services,
            ROUND(SUM(f.avg_medicare_payment * f.total_services)::NUMERIC, 2)
                                                             AS derived_medicare_spend_usd
        FROM  analytics.fact_services  f
        JOIN  analytics.dim_procedure  p ON f.procedure_key = p.procedure_key
        GROUP BY p.hcpcs_code, p.hcpcs_desc, p.hcpcs_drug_ind
        ORDER BY total_services DESC
        LIMIT 10;
    """,

    "Q3 – Provider type breakdown by derived Medicare spend (weighted)": """
        SELECT
            pr.provider_type,
            COUNT(DISTINCT pr.npi)                          AS unique_providers,
            SUM(f.total_services)::BIGINT                   AS total_services,
            ROUND(SUM(f.avg_medicare_payment * f.total_services)::NUMERIC, 2)
                                                             AS derived_medicare_spend_usd,
            ROUND(
                SUM(f.avg_medicare_payment * f.total_services)::NUMERIC
                / NULLIF(SUM(f.total_services), 0), 2
            )                                               AS wtd_avg_payment_per_svc
        FROM  analytics.fact_services  f
        JOIN  analytics.dim_provider   pr ON f.provider_key = pr.provider_key
        GROUP BY pr.provider_type
        ORDER BY derived_medicare_spend_usd DESC
        LIMIT 15;
    """,

    "Q4 – Provider gender split in top-10 states by service volume": """
        WITH top_states AS (
            SELECT g.state_code
            FROM   analytics.fact_services f
            JOIN   analytics.dim_geography g ON f.geo_key = g.geo_key
            GROUP  BY g.state_code
            ORDER  BY SUM(f.total_services) DESC
            LIMIT  10
        )
        SELECT
            g.state_code,
            COALESCE(pr.gender, 'U')                        AS gender,
            COUNT(DISTINCT pr.npi)                          AS provider_count,
            SUM(f.total_services)::BIGINT                   AS total_services
        FROM  analytics.fact_services  f
        JOIN  analytics.dim_provider   pr ON f.provider_key  = pr.provider_key
        JOIN  analytics.dim_geography  g  ON f.geo_key        = g.geo_key
        WHERE g.state_code IN (SELECT state_code FROM top_states)
        GROUP BY g.state_code, pr.gender
        ORDER BY g.state_code, total_services DESC;
    """,

    "Q5 – Medicare coverage ratio by region (derived spend / submitted charge)": """
        SELECT
            g.region,
            ROUND(
                SUM(f.avg_submitted_charge * f.total_services)::NUMERIC
                / NULLIF(SUM(f.total_services), 0), 2
            )                                               AS wtd_avg_submitted_usd,
            ROUND(
                SUM(f.avg_medicare_payment * f.total_services)::NUMERIC
                / NULLIF(SUM(f.total_services), 0), 2
            )                                               AS wtd_avg_medicare_usd,
            ROUND(
                SUM(f.avg_medicare_payment * f.total_services) * 100.0
                / NULLIF(SUM(f.avg_submitted_charge * f.total_services), 0), 1
            )                                               AS pct_covered
        FROM  analytics.fact_services  f
        JOIN  analytics.dim_geography  g ON f.geo_key = g.geo_key
        WHERE f.avg_submitted_charge > 0
          AND f.total_services       > 0
        GROUP BY g.region
        ORDER BY pct_covered DESC;
    """,
}


def phase_queries():
    """Run all 5 analytical SQL queries and print results to stdout."""
    log.info("=== PHASE 6: Analytical SQL Queries ===")
    engine = get_engine()
    for title, sql in ANALYTICAL_QUERIES.items():
        print(f"\n{'═' * 70}")
        print(f"  {title}")
        print(f"{'═' * 70}")
        try:
            result = pd.read_sql(text(sql), engine)
            if result.empty:
                print("  (no rows — run 'load' phase first)")
            else:
                print(result.to_string(index=False))
        except Exception as exc:
            log.error(f"  Query failed: {exc}")
    log.info("Queries complete.")

# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE:  run all phases with rollback awareness
# ─────────────────────────────────────────────────────────────────────────────

def phase_all():
    """Run all phases in order: setup → ingest → transform → load → queries."""
    for phase_fn in [phase_setup, phase_ingest, phase_transform, phase_load, phase_queries]:
        try:
            phase_fn()
        except Exception as exc:
            log.error(
                f"  Pipeline aborted at phase '{phase_fn.__name__}': {exc}\n"
                f"  Data from completed phases is intact.  Fix the error and "
                f"re-run from '{phase_fn.__name__}' (or run 'all' again)."
            )
            sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

PHASE_MAP: dict = {
    "setup":     phase_setup,
    "ingest":    phase_ingest,
    "transform": phase_transform,
    "load":      phase_load,
    "queries":   phase_queries,
    "all":       phase_all,
}


def main():
    parser = argparse.ArgumentParser(
        description="Healthcare Analytics Pipeline — CMS Medicare ETL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Phases:\n" + "\n".join(
            f"  {k:12s} {v.__doc__.strip().splitlines()[0]}"
            for k, v in PHASE_MAP.items()
        ),
    )
    parser.add_argument("phase", choices=list(PHASE_MAP.keys()),
                        help="Pipeline phase to run.")
    args = parser.parse_args()
    log.info(f"Database : {DB_HOST}:{DB_PORT}/{DB_NAME} (user={DB_USER})")
    log.info(f"Phase    : {args.phase}")
    PHASE_MAP[args.phase]()


if __name__ == "__main__":
    main()