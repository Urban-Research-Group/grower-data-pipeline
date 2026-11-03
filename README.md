# Grower Data Pipeline

An automated pipeline that retrieves raw per-county power outage data from S3, transforms it into **SAIDI** and **SAIFI** reliability metrics, and uploads the results back to S3 on a daily schedule.

---

## Table of Contents

- [Grower Data Pipeline](#grower-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Pipeline Architecture](#pipeline-architecture)
    - [Output Schema](#output-schema)
  - [Project Structure](#project-structure)
  - [Supported States](#supported-states)
  - [Setup and Installation](#setup-and-installation)
    - [Prerequisites](#prerequisites)
    - [Install dependencies](#install-dependencies)
    - [Configure AWS credentials (local)](#configure-aws-credentials-local)
  - [Running the Pipeline](#running-the-pipeline)
    - [Arguments](#arguments)
    - [Examples](#examples)
  - [The Mappings System](#the-mappings-system)
  - [CI/CD Behavior](#cicd-behavior)
    - [Triggers](#triggers)
    - [Bucket routing](#bucket-routing)
    - [Required GitHub Secrets](#required-github-secrets)
  - [Adding a New State](#adding-a-new-state)

---

## Overview

Each day, the Cloud Computing team scrapes per-county outage data to an S3 bucket (`urg-power-outage`). This pipeline:

1. Pulls the previous day's outage CSVs for a given state
2. Standardizes inconsistent column and county names across providers
3. Aggregates discrete outage events and computes SAIDI / SAIFI metrics per county
4. Attaches FIPS codes and formats the output
5. Uploads the final `{STATE}_DATA.csv` to the output S3 bucket (`state-metrics` for production, `state-metrics-dev` for the dev branch)

**SAIDI** (System Average Interruption Duration Index) measures the average total duration of interruptions per customer served, measured in hours affected per day.

**SAIFI** (System Average Interruption Frequency Index) measures the average number of interruptions per customer served. The pipeline produces three estimates — `lower`, `middle`, and `upper` — based on different methods of counting interrupted customers within each outage event, measured in unique outage events per day.

---

## Pipeline Architecture

The pipeline runs as a linear sequence of components defined in `main.py`. Each component receives a `DataWrapper` from the previous stage, performs its work, and passes a new `DataWrapper` to the next stage. Each available state runs in parallel, so each instance of the pipeline is responsible for only 1 state.

```
S3 (urg-power-outage)
        │
        ▼
┌─────────────────┐
│    Retrieve     │  File involved: retrieve.py
│                 │  Pulls per-county outage CSVs for the target state and date.
│                 │  Filters stale files, applies partial-download optimization
│                 │  for large files, and groups data by provider (EMC).
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Process      │  Files involved: process.py + standardize.py
│                 │  Standardizes column and county names using per-state
│                 │  mapping files. Groups timestamped rows into discrete
│                 │  outage events. Fills in counties with no reported outages
│                 │  using historical customers-served data.
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Metrics      │  File involved: metrics.py
│                 │  Computes SAIDI and SAIFI (lower/middle/upper) for every
│                 │  county in the state using the processed outage records.
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Cleaner      │  File involved: cleaner.py
│                 │  Joins FIPS codes from cleaned_FIPS.csv to each county
│                 │  and produces the final formatted output DataFrame.
└────────┬────────┘
         │  (skipped on --dry-run)
         ▼
┌─────────────────┐
│    Upload       │  uploader.py
│                 │  Uploads {STATE}_DATA.csv to the appropriate S3 bucket.
│                 │  Writes to state-metrics (main branch) or
│                 │  state-metrics-dev (dev branch).
└────────┬────────┘
         │
         ▼
S3 (state-metrics / state-metrics-dev)
```

### Output Schema

The final uploaded CSV has the following columns:

| Column        | Description                                 |
| ------------- | ------------------------------------------- |
| `County`      | Standardized county name                    |
| `FIPS`        | FIPS code for the county                    |
| `SAIDI`       | System Average Interruption Duration Index  |
| `LOWER_SAIFI` | SAIFI lower-bound estimate                  |
| `SAIFI`       | SAIFI mid estimate                          |
| `UPPER_SAIFI` | SAIFI upper-bound estimate                  |
| `State`       | Full state name                             |
| `Date`        | Date the metrics correspond to (YYYY-MM-DD) |

---

## Project Structure

```
grower-data-pipeline/
├── main.py                          # Entry point — builds and runs the pipeline
├── requirements.txt                 # Defines program dependencies
├── README.md
├── CONTRIBUTING.md                  # Git branching workflow guide
│
└── pipeline/
    ├── base.py                      # Component and DataWrapper base classes
    │
    ├── components/                  # One file per pipeline stage
    │   ├── retrieve.py              # Stage 1: pull from S3
    │   ├── process.py               # Stage 2: standardize + aggregate outages
    │   ├── standardize.py           # Helper used by process.py
    │   ├── metrics.py               # Stage 3: compute SAIDI/SAIFI
    │   ├── cleaner.py               # Stage 4: attach FIPS, format output
    │   └── uploader.py              # Stage 5: upload to S3
    │
    ├── mappings/                    # Per-state mapping files (see Mappings section)
    │   ├── guide.md                 # Explains each mapping file format
    │   ├── cleaned_FIPS.csv         # Master FIPS lookup used by cleaner.py
    │   └── {STATE}/                 # One folder per supported state (e.g. GA/, AL/)
    │
    └── historicalCustomersServed/   # Fallback customers-served CSVs per state
        └── {STATE}_customers_served.csv

```

---

## Supported States

| State Code | State          |
| ---------- | -------------- |
| `al`       | Alabama        |
| `fl`       | Florida        |
| `ga`       | Georgia        |
| `il`       | Illinois       |
| `la`       | Louisiana      |
| `ms`       | Mississippi    |
| `nc`       | North Carolina |
| `sc`       | South Carolina |

The set of states run by CI/CD is controlled by the `ALL_STATES` variable in `.github/workflows/pipeline.yml`.

---

## Setup and Installation

### Prerequisites

- Python 3.11
- AWS credentials with read access to `urg-power-outage` and write access to `state-metrics` / `state-metrics-dev`

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure AWS credentials (local)

Create a `.env` file in the project root:

```
AWS_ACCESS_KEY_ID=your_key_id
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
```

The pipeline loads this automatically via `python-dotenv`. The `.env` file is gitignored — never commit credentials.

---

## Running the Pipeline

```bash
python main.py <state> [options]
```

### Arguments

| Argument            | Description                                                                                                    |
| ------------------- | -------------------------------------------------------------------------------------------------------------- |
| `state`             | Required. Two-letter lowercase state code (e.g. `al`, `ga`)                                                    |
| `--dry-run`         | Run the full pipeline but skip the S3 upload. Saves output to `testing/{state}_baseline.csv` instead.          |
| `--date YYYY-MM-DD` | Process a specific date instead of yesterday.                                                                  |
| `--full-test`       | Run the full pipeline including upload, but force the destination to `state-metrics-dev` regardless of branch. |

### Examples

```bash
# Run for Alabama (processes yesterday's data, uploads to S3)
python main.py al

# Dry run for Georgia — no upload, saves CSV locally
python main.py ga --dry-run

# Reprocess a specific past date for Mississippi
python main.py ms --date 2025-11-15

# Full end-to-end test that uploads to the dev bucket
python main.py al --full-test
```

---

## The Mappings System

Every state requires a folder under `pipeline/mappings/{STATE}/` containing files that tell the pipeline how to interpret that state's raw data. Different providers use different column names and county name formats, so these files act as translation layers.

See [`pipeline/mappings/guide.md`](pipeline/mappings/guide.md) for a full description of each file format. The required files for each state are:

| File                     | Purpose                                                                         |
| ------------------------ | ------------------------------------------------------------------------------- |
| `col_map.csv`            | Maps raw column names to standardized column names                              |
| `raw_col_lists.csv`      | Lists all known raw column name variants per standardized column                |
| `county_map.csv`         | Maps raw county names to standardized county names                              |
| `county_map_verified.csv` | Optional hand-checked county mappings that override `county_map.csv`           |
| `master_county_list.txt` | The canonical list of county names for this state                               |
| `raw_county_list.txt`    | All raw county name variants seen in the source data                            |
| `raw_col_names.txt`      | Intermediate scratch file of raw column names (used when building new mappings) |

If a mapping file is missing or a county/column name is not covered, the pipeline will either skip that provider's data or produce NaN values for affected counties.

---

## CI/CD Behavior

The pipeline runs automatically via GitHub Actions (`.github/workflows/pipeline.yml`).

### Triggers

| Trigger                                      | States Processed                                                                                               |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Scheduled (daily at 11:00 UTC and 23:00 UTC) | All supported states                                                                                           |
| Push to `main`                               | All supported states                                                                                           |
| Push to `dev`                                | `al` only (default)                                                                                            |
| Manual dispatch (`workflow_dispatch`)        | Specify a JSON array of states, e.g. `["al","ga"]`. Falls back to the `STATE_MATRIX` repo variable, then `al`. |

### Bucket routing

| Branch                      | Destination Bucket           |
| --------------------------- | ---------------------------- |
| `main`                      | `state-metrics` (production) |
| `dev` (or any other branch) | `state-metrics-dev`          |

### Required GitHub Secrets

The following secrets must be set in the repository settings under **Settings → Secrets and variables → Actions**:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

Each state runs as a parallel job in a matrix, so states do not block each other.

---

## Adding a New State

1. Create a folder `pipeline/mappings/{STATE}/` (use the two-letter uppercase state code)
2. Add all required mapping files — see the [Mappings System](#the-mappings-system) section and [`pipeline/mappings/guide.md`](pipeline/mappings/guide.md)
3. Add a `pipeline/historicalCustomersServed/{STATE}_customers_served.csv` with county-level historical customers-served values (used as fallback for counties with no outage activity)
4. Add the lowercase state code to the `ALL_STATES` list in `.github/workflows/pipeline.yml`
5. Run `python main.py {state} --dry-run` locally to verify the mapping files are correct before merging
