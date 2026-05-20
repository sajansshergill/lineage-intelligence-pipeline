# Financial Data Lineage &amp; Quality Intelligence Pipeline

A production-grade PySpark ETL framework with column-level lineage tracking, declarative data quality enforcement, and an AI-powered lineage advisor — built to mirror the data platform challenges in financial services engineering.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Quality Rules](#data-quality-rules)
- [Lineage Graph](#lineage-graph)
- [Dashboard & AI Advisor](#dashboard--ai-advisor)
- [Job Definitions](#job-definitions)
- [Running Tests](#running-tests)
- [JD Alignment Map](#jd-alignment-map)
- [Roadmap](#roadmap)

## Overview
Financial data pipelines fail silently. A wrong notional value in a raw trade feed can corrupt a risk report three hops downstream —— and by the time someone noties, the lineage is gone. This project solves that.

The Financial Data Lienage & Quality Intelligence Pipeline is a configurable, fault-tolerant ETL framework built on PySpark that:
1. Ingests raw trade and position data from landing zone files
2. Applies a declarative data quality rule engine at every stage
3. Tracks column-level lineage as a directed graph (NetworkX / Stardog)
4. Persists violations and run metadata to DuckDB
5. Surfaces everything on a Streamlit Dashboard with a Cluade-powered AI lineage advisor

This project targets the engineering challenges faced by data platform teams at firms like Morgan Stanley —— where every figure in a risk report must be traceable back to its source, and L3 support means knowing exactly which upstream field broke a downstream calculation.

## Architecture
<img width="387" height="901" alt="image" src="https://github.com/user-attachments/assets/743aa9d4-a624-4d49-b4b0-77ec923ea714" />

## Key Features
**PySpark ETL Framework**
- Abstract BaseTransform class with a transfrom registry —— every column-level operation is named and versioned
- Configurable pipeline stages via YAML: ingest -> validate -> clean -> enrich -> load
- Dead-letter queue: records failining DQ checks are routed to a separate partition with failure reason, timestamp, and originating rule
- Fault-tolerant execution: stage failures are caught, logged, and retired with exponential backoff before making the job as failed

**Data Quality Rules Engine**
-  Declarative rules defined in rules_config.yaml: null checks, range validators, referential integrity, stale-data detection, cross-field consistency
-  Rules are executed at configurable pipeline checkpoints (post-ingest, post-clean, pre-load)
-  Every violation is logged to DuckDB with: rule name, severity (WARN / ERROR / CRITICAL), field name, offending value, row key, and batch ID
-  DQ score (0-100) computed per batch and stored in run history

**Column-Level Lineage Tracking**
- Every transform in the registry emits a (source_col, target_col, transform_name, stage) edge
- Edges are assembled into a directed acyclic graph using NetworkX
- Optional persistence to a **Stardog** (community edition) via SPARQL/RDF for production-grade graph querying
- Lineage is queryable: <em>"What upstream fields affect risk_report.exposure"</em>

**Kimball Star Schema**
- FactTrades: trade_id, product_key. counterparty_key, date_key, notional_usd, quantity, direction, settlement_status
- DimProduct: product_key, ticker, asset_class, currency, exchange
- DimCounterparty: counterparty_key, legal_entity, lei_code, country, credit_rating
- DimDate: date_key, calendar_date, quarter, fiscal_year, is_trading_day

**Autosys-Style Job Definitions**
- Job specs defined as JSON: job name, dependencies, schedule (cron), max runtime, alert thresholds
- Python executor parses and runs jobs in dependency order, respecting upstream success gates

**Streamlit Dashboard**
- **Run History:** success/fail per batch, DQ score trend, records processed vs rejected
- **Lineage Explorer:** click any field to trace full upstream ancestry and downstream impact
- **DQ Violation Drilldown:** filter by rule, severity, date range, export to CSV
- **Claude AI Advisor Tab:** natural language interface over the lineage graph

**AI Advisor (Claude API)**
- Powered by claude-sonnet-4-20250514
- Lineage graph context injected ad structured JSON into each prompt
- Example queries handled:
  - <em>"Which source field is responsible for the EUR exposure figure?"
  - "Show me all fields that pass through the currency conversion transform"
  - "What DQ rules failed in yesterday's batch and what did they affect downstream?"</em>

## Tech Stack
<img width="587" height="510" alt="image" src="https://github.com/user-attachments/assets/5748560c-b17b-4f51-a322-06abc60460cc" />

## Project Structure
<img width="373" height="590" alt="image" src="https://github.com/user-attachments/assets/2490096b-71c4-4fe6-9cc9-318f48056308" />



<img width="373" height="617" alt="Screenshot 2026-05-15 at 11 07 15 PM" src="https://github.com/user-attachments/assets/087362fc-0fd8-4ae9-92d0-9c30778d32f8" />

## Getting Started
**Prerequisites**
- Python 3.11+
- Java 11+ (required for PySpark)
- Docker & Docker Compose
- Anthropic API key (for the AI advisor tab)
- Stardog Community Edition (optional — NetworkX used by default)

### 1. Clone and configure
git clone https://github.com/sajanshergill/financial-lineage-pipeline.git
cd financial-lineage-pipeline
cp config/env.example .env
##### Edit .env: set ANTHROPIC_API_KEY, STARDOG_URL (if using), DUCKDB_PATH

**2. Bootstrap the environment**
bash scripts/bootstrap.sh
#### Creates virtualenv, installs requirements, sets JAVA_HOME

**3. Generate sample data**
python scripts/generate_sample_data.py --trades 50000 --positions 10000
#### Outputs to data/raw/trades_YYYYMMDD.csv and positions_YYYYMMDD.csv

**4. Run the pipeline**
python jobs/job_executor.py --job daily_trade_pipeline
#### Reads job_definitions.json, resolves dependencies, runs stages in order

Or run a single stage directly:
python etl/framework.py --stage clean --input data/raw/trades_20250515.csv

**5. Launch the dashboard**
streamlit run dashboard/app.py
### Open http://localhost:8501

**6. (Optional) Start with Docker**
docker-compose up --build
### Runs pipeline + dashboard + DuckDB volume

## Deployment
This repository is ready for automatic deployment from GitHub with Streamlit Community Cloud.

**Streamlit Cloud setup**
1. Push the repository to GitHub.
2. In Streamlit Cloud, create a new app from the GitHub repo.
3. Set the main file path to `dashboard/app.py`.
4. Use the default branch `main`.
5. Add optional secrets:
   - `ANTHROPIC_API_KEY` for the AI Advisor tab
   - `ANTHROPIC_MODEL` if you want to override the default Claude model

After the app is connected, Streamlit Cloud redeploys automatically on every push to `main`. The `.streamlit/config.toml` file supplies cloud-friendly Streamlit defaults, and `packages.txt` installs OpenJDK for PySpark-compatible environments.

GitHub Actions also runs on every push and pull request to `main` via `.github/workflows/ci.yml`. The workflow installs dependencies, compiles key modules, runs tests, and smoke-checks the Streamlit entry point.

**Docker deployment**
The app can also run with Docker:
```bash
docker compose up --build dashboard
```

## Data Quality Rules
Rules are declared in dq/rules_config.yaml:
rules:
  - name: notional_not_null
    field: notional
    type: null_check
    severity: CRITICAL
    stage: post_ingest

  - name: notional_positive
    field: notional
    type: range_check
    min: 0.01
    max: 1_000_000_000
    severity: ERROR
    stage: post_clean

  - name: currency_valid
    field: currency
    type: referential_integrity
    lookup_table: dim_product
    lookup_field: currency
    severity: ERROR
    stage: post_ingest

  - name: settlement_date_not_before_trade_date
    fields: [trade_date, settlement_date]
    type: cross_field
    rule: settlement_date >= trade_date
    severity: WARN
    stage: post_clean

Each violation is logged:
<img width="653" height="187" alt="image" src="https://github.com/user-attachments/assets/5de78adf-6d53-4b14-b4db-6c2d8c50f66a" />

## Lineage Graph
Every transform emits a lineage edge:
### Inside enrich.py
@register_transform("fx_convert")
def fx_convert(df: DataFrame, tracker: LineageTracker) -> DataFrame:
    tracker.emit(source="notional", target="notional_usd", transform="fx_convert", stage="enrich")
    # ... PySpark transform logic
    return df

### Resulting lineage path:
<img width="647" height="121" alt="image" src="https://github.com/user-attachments/assets/6227f585-fd5e-440e-b0f1-454170c46430" />

Query in Python:
from graph.queries import trace_ancestry
ancestry = trace_ancestry("risk_report.eur_exposure")
#### Returns: ['raw.notional', 'enriched.notional_usd', 'risk_report.usd_exposure', 'risk_report.eur_exposure']

Query in SPARQL (Stardog):
SELECT ?source ?transform WHERE {
  ?edge fin:target "risk_report.eur_exposure" ;
        fin:source ?source ;
        fin:transform ?transform .
}

## Dashboard & AI Advisor
The AI Advisor tab accepts natural language and responds using lineage graph context injected at runtime:
#### dashboard/pages/ai_advisor.py (simplified)

lineage_context = graph.to_json()  #### Serialized lineage graph

response = anthropic.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1000,
    system="""You are a financial data lineage expert. 
    Answer questions about data provenance using the lineage graph provided.
    Be precise about field names, transform names, and pipeline stages.""",
    messages=[
        {
            "role": "user",
            "content": f"Lineage graph:\n{lineage_context}\n\nQuestion: {user_query}"
        }
    ]
)

**Example advisor queries:**
- <em>"Which source field is responsible for the EUR exposure figure in today's risk report?"
- "If the FX rate lookup fails, which downstream fields are impacted?"
- "Show me all transforms that touch the counterparty LEI field"</em>

## Job Definitions
jobs/job_definitions.json:
{
  "jobs": [
    {
      "name": "ingest_trades",
      "command": "etl/framework.py --stage ingest",
      "schedule": "0 6 * * 1-5",
      "max_runtime_minutes": 30,
      "on_failure": "alert_l3"
    },
    {
      "name": "run_dq_checks",
      "command": "dq/rules_engine.py --batch latest",
      "depends_on": ["ingest_trades"],
      "max_runtime_minutes": 15,
      "on_failure": "alert_l3"
    },
    {
      "name": "build_lineage",
      "command": "graph/lineage_graph.py --batch latest",
      "depends_on": ["run_dq_checks"],
      "max_runtime_minutes": 10
    },
    {
      "name": "daily_trade_pipeline",
      "type": "group",
      "jobs": ["ingest_trades", "run_dq_checks", "build_lineage"]
    }
  ]
}

## Running Tests
pytest tests/ -v --tb=short
Test coverage includes:
<img width="647" height="243" alt="image" src="https://github.com/user-attachments/assets/e812c4d6-4e6b-4997-9a6b-baacef254a83" />

pytest tests/test_dq_rules.py -v
### 24 tests: null_check (6), range_check (6), referential_integrity (6), cross_field (6)

## JD Alignment Map
<img width="659" height="598" alt="image" src="https://github.com/user-attachments/assets/4ac8d031-a9d2-4cdd-884d-010e855f9bee" />

## Roadmap
- Add Kafka ingestion layer for real-time trade feed simulation
- Integrate Power Designer export for ERD diagram generation
- Add Java UDF bridge for PySpark custom aggregations
- Expand Stardog schema with OWL ontology for financial instrument types
- Add MLflow tracking for DQ score trends over time
- CI/CD: auto-run pipeline smoke test on push with Github Actions

Built to demonstrate production-grade financial data engineering — PySpark ETL, data lineage, DQ enforcement, and AI-assisted provenance querying.Share
