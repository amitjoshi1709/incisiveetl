# Orders ETL Service - Technical Documentation

**Repository:** `orders-etl-service`
**Runtime:** Node.js (JavaScript, ES6+)
**Branch:** `main`

---

## Project Structure

```
orders-etl-service/
├── index.js                          # Main entry point and CLI handler
├── package.json                      # Dependencies and npm scripts
├── .env.example                      # Environment variable template
├── .gitignore                        # Git ignore rules
├── README.md                         # Basic project readme
├── logs/                             # Local log output directory
│
└── src/
    ├── config/
    │   └── index.js                  # Centralized configuration loader
    │
    ├── core/
    │   ├── index.js                  # Core module exports
    │   ├── BasePipeline.js           # Abstract base class for all pipelines
    │   ├── DatabaseConnection.js     # PostgreSQL connection pool manager
    │   └── S3Handler.js              # AWS S3 operations handler
    │
    ├── etl/
    │   └── Orchestrator.js           # Central coordinator for pipeline execution
    │
    ├── pipelines/
    │   ├── index.js                  # Auto-discovery pipeline registry
    │   ├── _template/                # Template for creating new pipelines
    │   │   └── index.js
    │   ├── orders/                   # Orders pipeline (staging + merge)
    │   │   └── index.js
    │   ├── product-catalog/          # Product catalog reference data
    │   │   └── index.js
    │   ├── dental-practices/         # Dental practices reference data
    │   │   └── index.js
    │   ├── dental-groups/            # Dental groups reference data
    │   │   └── index.js
    │   ├── lab-product-mapping/      # Lab-to-product mapping
    │   │   └── index.js
    │   ├── lab-practice-mapping/     # Lab-to-practice mapping
    │   │   └── index.js
    │   ├── product-lab-markup/       # Product pricing/markup by lab
    │   │   └── index.js
    │   └── product-lab-rev-share/    # Revenue share schedules
    │       └── index.js
    │
    └── utils/
        ├── logger.js                 # Winston logging configuration
        └── hash.js                   # MD5 hash utility for deduplication
```

---

## 1. ETL Overview

### Purpose of the Pipeline

- Extracts CSV files from AWS S3 bucket folders
- Transforms and validates rows according to pipeline-specific rules
- Loads validated data into PostgreSQL database tables
- Produces audit logs (success/error) back to S3 for traceability

### Business Use Case

- Serves the dental industry vertical
- Processes dental lab order transactions from upstream lab management systems
- Maintains reference data for products, practices, groups, and pricing
- Primary entity: **orders** (dental lab case orders with patient, product, and shipping details)
- Supporting entities: product catalogs, dental practices/groups, lab mappings, pricing schedules

---

## 2. Data Sources

### Source Systems

- **AWS S3** — single bucket with multiple prefixes (folders), one per pipeline
- CSV files are deposited by an external upstream system (not part of this repo)
- Only files ending in `.csv` are processed
- Sub-folders `processed/` and `logs/` are excluded from processing

### S3 Folder Structure (per pipeline)

```
s3://<bucket>/<SOURCEPATH>/
├── file1.csv                    ← Picked up for processing
├── file2.csv
├── processed/                   ← Valid-row CSVs land here after processing
│   └── 2025-01-15T10-30-00-000Z_file1.csv
└── logs/                        ← Audit log CSVs land here after processing
    └── file1_log_2025-01-15T10-30-00-000Z.csv
```

### Data Extraction Method

- `S3Handler.listFiles()` paginates through `ListObjectsV2` for each source prefix
- Filters out folder markers, `processed/`, `logs/`, and non-CSV files
- Each file is retrieved via `GetObjectCommand` as a stream
- Stream is piped into `csv-parser` library for parsing

### Pipeline-to-S3-Prefix Mapping

- **orders** → env var `SOURCEPATH` (default: `dev_orders`)
- **product-catalog** → env var `PRODUCT_CATALOG_SOURCEPATH` (default: `dev_product_catalog`)
- **dental-practices** → env var `DENTAL_PRACTICES_SOURCEPATH` (default: `dev_dental_practices`)
- **dental-groups** → env var `DENTAL_GROUPS_SOURCEPATH` (default: `dev_dental_groups`)
- **lab-product-mapping** → env var `LAB_PRODUCT_MAPPING_SOURCEPATH` (default: `dev_lab_product_mapping`)
- **lab-practice-mapping** → env var `LAB_PRACTICE_MAPPING_SOURCEPATH` (default: `dev_lab_practice_mapping`)
- **product-lab-markup** → env var `PRODUCT_LAB_MARKUP_SOURCEPATH` (default: `dev_product_lab_markup`)
- **product-lab-rev-share** → env var `PRODUCT_LAB_REV_SHARE_SOURCEPATH` (default: `dev_product_lab_rev_share`)

---

## 3. Transformations

### Common Processing Flow (BasePipeline)

All pipelines inherit this sequence from `src/core/BasePipeline.js`:

- **Parse** — Stream CSV through `csv-parser` into array of row objects
- **Normalize** — Lowercase all column headers, strip non-alphanumeric characters
  - Example: `CaseId` → `caseid`, `Lab Product ID` → `labproductid`
- **Map** — Pipeline-specific `mapRow()` transforms normalized keys to DB column names
- **Validate** — Check that all `requiredFields` are present and non-empty
- **Insert** — Each row inserted with PostgreSQL `SAVEPOINT` for individual error recovery
- **Post-process** — Optional hook (only orders pipeline uses this to call stored procedure)

### Pipeline-Specific Transformations

**orders** (`src/pipelines/orders/index.js`)
- Target table: `orders_stage`
- Truncates staging table before each run (full refresh)
- Required fields: `submissiondate`, `casedate`, `caseid`, `productid`, `quantity`, `customerid`
- Type conversions: `quantity` → parseInt, `caseid` → parseInt
- Generates MD5 `row_hash` for deduplication via `generateRowHash()`
- Records `source_file_key` for data lineage
- Post-process: calls `CALL merge_orders_stage()` stored procedure

**product-catalog** (`src/pipelines/product-catalog/index.js`)
- Target table: `incisive_product_catalog`
- Required fields: `incisive_id`, `incisive_name`, `category`
- Key mappings: `incisiveid` → `incisive_id` (Number), `incisivename` → `incisive_name`, `subcategory` → `sub_category`
- Conflict handling: `ON CONFLICT (incisive_id) DO NOTHING`

**dental-practices** (`src/pipelines/dental-practices/index.js`)
- Target table: `dental_practices`
- Required fields: `practice_id`, `dental_group_id`
- Key mappings: `practiceid` → `practice_id` (Number), `dentalgroupid` → `dental_group_id` (Number)
- Conflict handling: `ON CONFLICT (practice_id) DO NOTHING`

**dental-groups** (`src/pipelines/dental-groups/index.js`)
- Target table: `dental_groups`
- Required fields: `dental_group_id`, `name`
- Key mappings: `dentalgroupid` → `dental_group_id` (Number)
- Boolean parsing: `centralizedbilling` → `centralized_billing` (`'true'`/`'1'` = true)
- Conflict handling: `ON CONFLICT (dental_group_id) DO NOTHING`

**lab-product-mapping** (`src/pipelines/lab-product-mapping/index.js`)
- Target table: `lab_product_mapping`
- Required fields: `lab_id`, `lab_product_id`, `incisive_product_id`
- Key mappings: `labid` → `lab_id` (Number), `incisiveproductid` → `incisive_product_id` (Number)
- Conflict handling: `ON CONFLICT (lab_id, lab_product_id) DO NOTHING`

**lab-practice-mapping** (`src/pipelines/lab-practice-mapping/index.js`)
- Target table: `lab_practice_mapping`
- Required fields: `lab_id`, `practice_id`, `lab_practice_id`
- Key mappings: `labid` → `lab_id` (Number), `practiceid` → `practice_id` (Number)
- Conflict handling: `ON CONFLICT (lab_id, lab_practice_id) DO NOTHING`

**product-lab-markup** (`src/pipelines/product-lab-markup/index.js`)
- Target table: `product_lab_markup`
- Required fields: `lab_id`, `lab_product_id`
- Pricing fields: `cost`, `standardprice`, `nfprice` → parseFloat
- Boolean parsing: `commitmenteligible` → `commitment_eligible` (`'true'`/`'1'`/`'yes'` = true)
- Conflict handling: `ON CONFLICT (lab_id, lab_product_id) DO NOTHING`

**product-lab-rev-share** (`src/pipelines/product-lab-rev-share/index.js`)
- Target table: `product_lab_rev_share`
- Required fields: `lab_id`, `lab_product_id`, `fee_schedule_name`
- Key mappings: `revenueshare` → `revenue_share` (parseFloat)
- Boolean parsing: `commitmenteligible` → `commitment_eligible`
- Conflict handling: `ON CONFLICT (lab_id, lab_product_id, fee_schedule_name) DO NOTHING`

### Validation and Cleansing Rules

- **Required field validation** — Each pipeline declares mandatory columns; rows missing any are rejected
- **Type coercion** — parseInt/parseFloat applied where specified; NaN values pass through (no explicit guard)
- **Duplicate handling** — Reference data pipelines use `ON CONFLICT DO NOTHING`; orders pipeline uses `row_hash` + merge procedure
- **Empty string handling** — Treated as missing for required field validation

---

## 4. Data Destinations

### Target Database

- **Engine:** PostgreSQL
- **Schema:** `etl` (with fallback to `public` via `search_path=etl,public`)
- **Connection:** `pg.Pool` with configurable pool settings

### Database Connection Settings

- Max connections: 10 (configurable via `DB_POOL_MAX`)
- Idle timeout: 30 seconds (configurable via `DB_IDLE_TIMEOUT`)
- Connect timeout: 2 seconds (configurable via `DB_CONNECT_TIMEOUT`)
- SSL: Optional (configurable via `DB_SSL`)

### Target Tables and Load Strategies

- **orders_stage** (orders pipeline)
  - Load strategy: Truncate-and-load (full refresh each run)
  - Post-load: `CALL merge_orders_stage()` merges into final orders table

- **incisive_product_catalog** (product-catalog pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

- **dental_practices** (dental-practices pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

- **dental_groups** (dental-groups pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

- **lab_product_mapping** (lab-product-mapping pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

- **lab_practice_mapping** (lab-practice-mapping pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

- **product_lab_markup** (product-lab-markup pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

- **product_lab_rev_share** (product-lab-rev-share pipeline)
  - Load strategy: Insert, skip on conflict (upsert)

### Stored Procedures

- **`merge_orders_stage()`** — Called after orders pipeline inserts into staging
  - Merges staging data into final orders table
  - Procedure definition is in PostgreSQL, NOT in this repository
  - Must be inspected directly in the database

### Schema Notes

- DDL/migration scripts are NOT included in this repository
- Table schemas must be obtained from the database directly
- The `orders_stage` table has 31 columns including ETL-added fields (`source_file_key`, `row_hash`)

---

## 5. Scheduling & Execution

### How Jobs Run

- **No built-in scheduler** — This is a single-run CLI process
- Executes, processes all available CSV files, then exits
- External scheduling required (cron, CloudWatch Events, manual trigger)

### CLI Commands

```bash
# Process all pipelines (default)
node index.js

# Process all pipelines (explicit)
node index.js all

# Process a single pipeline
node index.js orders
node index.js dental-groups
node index.js product-catalog

# List available pipelines
node index.js list
```

### Execution Characteristics

- Pipelines run sequentially (one after another, not parallel)
- Files within a pipeline are processed sequentially
- Each file is processed inside a single PostgreSQL transaction
- Process exits with code 0 (success) or 1 (fatal error)

### Manual vs Automated Execution

- **Manual:** Run `node index.js` from command line
- **Automated:** No automation exists in this repo; must be configured externally
  - Options: cron job, AWS CloudWatch Events, ECS scheduled tasks, Lambda
  - Previous team's scheduling mechanism is unknown

---

## 6. Configuration

### Environment Variables

**AWS / S3 Configuration**
- `AWS_REGION` — AWS region (default: `us-east-1`)
- `S3_BUCKET` — S3 bucket name (required)
- `AWS_ACCESS_KEY_ID` — AWS access key (optional if using IAM role)
- `AWS_SECRET_ACCESS_KEY` — AWS secret key (optional if using IAM role)

**Database Configuration**
- `DB_HOST` — PostgreSQL host (default: `localhost`)
- `DB_PORT` — PostgreSQL port (default: `5432`)
- `DB_USER` — Database username (required)
- `DB_PASSWORD` — Database password (required)
- `DB_NAME` — Database name (default: `postgres`)
- `DB_SSL` — Enable SSL connection (default: `false`)
- `DB_POOL_MAX` — Max pool connections (default: `10`)
- `DB_IDLE_TIMEOUT` — Idle timeout in ms (default: `30000`)
- `DB_CONNECT_TIMEOUT` — Connect timeout in ms (default: `2000`)

**Pipeline S3 Paths**
- `SOURCEPATH` — Orders pipeline S3 prefix
- `PRODUCT_CATALOG_SOURCEPATH` — Product catalog S3 prefix
- `DENTAL_PRACTICES_SOURCEPATH` — Dental practices S3 prefix
- `DENTAL_GROUPS_SOURCEPATH` — Dental groups S3 prefix
- `LAB_PRODUCT_MAPPING_SOURCEPATH` — Lab product mapping S3 prefix
- `LAB_PRACTICE_MAPPING_SOURCEPATH` — Lab practice mapping S3 prefix
- `PRODUCT_LAB_MARKUP_SOURCEPATH` — Product lab markup S3 prefix
- `PRODUCT_LAB_REV_SHARE_SOURCEPATH` — Product lab rev share S3 prefix

**Logging Configuration**
- `LOG_LEVEL` — Winston log level (default: `info`)
- `LOG_TO_CONSOLE` — Enable console output (default: `true`)
- `BATCH_SIZE` — Rows per processing batch (default: `100`)

### Credentials Handling

- **Database credentials:** Plaintext in `.env` file (file is in `.gitignore`)
- **AWS credentials:** Either plaintext in `.env` OR via IAM role/instance profile
  - S3Handler checks for explicit credentials first
  - Falls back to AWS default credential chain if not provided
- **No secrets manager integration** — Must be handled at infrastructure level

---

## 7. Deployment

### Current State

- No Dockerfiles in repository
- No CI/CD pipeline configuration
- No infrastructure-as-code (Terraform, CloudFormation)
- No deployment scripts
- Deployment appears to be manual

### How to Set Up a New Environment

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd orders-etl-service
   npm install
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   # Edit .env with environment-specific values
   ```

3. **Prepare PostgreSQL database**
   - Create the `etl` schema
   - Create all target tables (DDL not in repo)
   - Deploy the `merge_orders_stage()` stored procedure

4. **Prepare S3 bucket**
   - Ensure bucket exists with configured name
   - Configure IAM permissions for the credentials being used
   - Create source folders for each pipeline

5. **Run the service**
   ```bash
   npm start          # Production run
   npm run dev        # Development with auto-reload (nodemon)
   ```

### DEV vs PROD Differentiation

- Controlled entirely by `.env` file contents
- Different S3 prefixes (e.g., `dev_orders` vs `prod_orders`)
- Different database hosts/credentials
- No code-level environment checks (`NODE_ENV` not used)

---

## 8. Failure Handling & Monitoring

### Retry Logic

- **No automatic retry logic exists**
- If a file fails, it remains in the S3 source folder
- Original file deletion is **commented out** (`Orchestrator.js:267`)
- De facto retry: next run will re-process the same files

### Transaction Behavior

- Each CSV file processed within a single `BEGIN`/`COMMIT` transaction
- Each row insert uses `SAVEPOINT` for individual error recovery
- Failed row: savepoint rolls back, error recorded, transaction continues
- Failed `postProcess` (e.g., merge procedure): entire transaction rolls back
- Unhandled exception during processing: entire transaction rolls back

### Error Reporting to S3

For every processed file, two output CSVs are uploaded:

**Processed file** (`processed/{timestamp}_{filename}.csv`)
- Contains only valid/successfully-inserted rows
- Uploaded to the pipeline's `processed/` subfolder

**Log file** (`logs/{baseName}_log_{timestamp}.csv`)
- Contains ALL rows (valid + invalid)
- Includes three appended columns:
  - `etl_status`: `'success'` or `'error'`
  - `etl_reason`: Error message (empty for successful rows)
  - `missingFields`: Comma-separated list of missing required fields

### Local Logging

- **Logger:** Winston with JSON-formatted output
- **Global logs:**
  - `logs/combined.log` — All log levels
  - `logs/error.log` — Errors only
- **Pipeline-specific logs:**
  - `logs/<pipeline-name>/combined.log`
  - `logs/<pipeline-name>/error.log`
- **Console output:** Enabled when `LOG_TO_CONSOLE=true`

### Alerts

- **No alerting system exists**
- No integration with PagerDuty, Slack, email, or CloudWatch Alarms
- Monitoring must be built externally
  - Check log files
  - Monitor S3 processed/logs folders
  - Wrap process in scheduler that reports exit codes

---

## 9. Operational Playbook

### How to Re-Run Jobs

**Re-run all pipelines:**
```bash
node index.js
# or
node index.js all
```

**Re-run a specific pipeline:**
```bash
node index.js orders
node index.js dental-groups
node index.js product-catalog
```

**Re-process a specific file:**
1. Ensure the file exists in the S3 source folder (`s3://<bucket>/<prefix>/`)
2. Remove other files if you want to isolate the run
3. Run: `node index.js <pipeline-name>`

**Note:** Source file deletion is disabled — files remain after processing. Manual cleanup needed to prevent re-processing.

### How to Fix Failed Pipelines

**Scenario: Rows failed validation (missing required fields)**
1. Check S3 logs folder for `*_log_*.csv` file
2. Filter rows where `etl_status = 'error'`
3. Review `etl_reason` and `missingFields` columns
4. Fix source data upstream
5. Re-drop corrected file into S3 source folder
6. Re-run the pipeline

**Scenario: Database insert failures (constraint violations)**
1. Check `logs/<pipeline-name>/error.log`
2. Check S3 log CSV for `etl_status = 'error'` rows
3. Review Postgres error message in `etl_reason`
4. Investigate schema mismatch or data issues
5. Fix and re-run

**Scenario: Transaction rolled back (postProcess failure)**
- Applies only to orders pipeline (`merge_orders_stage()`)
- Entire file's inserts are rolled back if procedure fails
1. Check `logs/orders/error.log` for procedure error
2. Connect to PostgreSQL and inspect the stored procedure
3. Fix procedure or data issue
4. Re-run orders pipeline

**Scenario: S3 connectivity errors**
1. Verify `S3_BUCKET` and source path prefix in `.env`
2. Verify AWS credentials are valid
3. Test AWS CLI access: `aws s3 ls s3://<bucket>/<prefix>/`
4. Check IAM permissions

**Scenario: Database connectivity issues**
1. Verify `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
2. Test connection: `psql -h <host> -U <user> -d <dbname>`
3. If using SSL, verify `DB_SSL=true` is set
4. Check for pool exhaustion (max 10 connections, 2s timeout)

### How to Add a New Pipeline

1. Copy template folder:
   ```bash
   cp -r src/pipelines/_template src/pipelines/<new-name>
   ```

2. Edit `src/pipelines/<new-name>/index.js`:
   - Implement `get name()` — pipeline identifier
   - Implement `get tableName()` — target database table
   - Implement `get requiredFields()` — array of mandatory columns
   - Implement `get envKey()` — environment variable name for S3 path
   - Implement `mapRow(normalizedRow)` — CSV to DB column mapping
   - Implement `buildInsertQuery(mappedRow)` — SQL insert statement

3. Add environment variable to `.env`:
   ```
   NEW_PIPELINE_SOURCEPATH=dev_new_pipeline
   ```

4. Create target table in PostgreSQL

5. Restart the service — pipeline auto-registers via filesystem discovery

### Key Operational Caveats

- **Source file deletion is disabled** — Files remain in S3 source folder after processing (`Orchestrator.js:267`). Without manual cleanup, every run re-processes all files.

- **Orders pipeline idempotency** — Truncates staging table on every run, then merges. If merge fails mid-way and process re-runs, staging will be truncated and reloaded. Merge procedure must handle this.

- **No test suite** — No unit or integration tests exist. Validate changes manually in dev environment.

- **No database migrations** — Schema changes must be applied manually. No migration framework.

- **`merge_orders_stage()` is external** — Stored procedure not in this repo. Behavior must be understood by inspecting the database.

---

## Appendix: Dependencies

**Production Dependencies**
- `@aws-sdk/client-s3` (^3.450.0) — AWS S3 API client
- `csv-parser` (^3.0.0) — Streaming CSV parser
- `dotenv` (^16.3.1) — Environment variable loader
- `pg` (^8.11.3) — PostgreSQL client and connection pool
- `winston` (^3.11.0) — Structured logging

**Development Dependencies**
- `nodemon` (^3.0.2) — Auto-restart on file changes
