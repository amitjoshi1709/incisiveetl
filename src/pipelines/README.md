# ETL Pipelines

This folder contains all ETL pipelines for the service. Each pipeline is self-contained in its own folder.

## Adding a New Pipeline

### Step 1: Copy the Template

```bash
# Copy the template folder
cp -r _template my-new-pipeline
```

### Step 2: Update the Pipeline Class

Edit `my-new-pipeline/index.js`:

1. **Rename the class**: `TemplatePipeline` → `MyNewPipeline`
2. **Update static name**: `static pipelineName = 'my-new-pipeline'`
3. **Implement getters**:
   - `name`: Pipeline identifier (match folder name)
   - `tableName`: Target database table
   - `requiredFields`: Array of mandatory fields
   - `envKey`: Environment variable name for S3 path
4. **Implement methods**:
   - `mapRow()`: Transform CSV columns to DB columns
   - `buildInsertQuery()`: Generate INSERT SQL

### Step 3: Add Environment Variable

Add to your `.env` file:
```
MY_NEW_PIPELINE_SOURCEPATH=path/to/files
```

### Step 4: Restart

The pipeline auto-registers on startup!

```bash
node index.js list           # Should show your new pipeline
node index.js my-new-pipeline  # Run it
```

## Pipeline Structure

```
pipelines/
├── index.js              # Auto-discovery registry
├── README.md             # This file
├── _template/            # Copy this to create new pipelines
│   └── index.js
├── orders/
│   └── index.js
├── product-catalog/
│   └── index.js
├── dental-practices/
│   └── index.js
├── lab-product-mapping/
│   └── index.js
├── lab-practice-mapping/
│   └── index.js
└── dental-groups/
    └── index.js
```

## Required vs Optional Overrides

### Required (Must Implement)

| Property/Method | Description |
|-----------------|-------------|
| `static pipelineName` | Unique identifier (should match folder name) |
| `get name()` | Pipeline name for logging |
| `get tableName()` | Target database table |
| `get requiredFields()` | Fields that must be non-empty |
| `get envKey()` | Environment variable for S3 path |
| `mapRow(row)` | Transform CSV row to DB schema |
| `buildInsertQuery(row)` | Generate INSERT SQL |

### Optional (Override if Needed)

| Property/Method | Default | Description |
|-----------------|---------|-------------|
| `get shouldTruncate()` | `false` | Truncate table before processing |
| `postProcess(client)` | No-op | Run after all rows inserted |
| `truncateTable(client)` | `TRUNCATE TABLE {tableName}` | Custom truncate logic |

## Example Pipeline

```javascript
const BasePipeline = require('../../core/BasePipeline');

class MyPipeline extends BasePipeline {
    static pipelineName = 'my-pipeline';

    get name() { return 'my-pipeline'; }
    get tableName() { return 'my_table'; }
    get requiredFields() { return ['id', 'name']; }
    get envKey() { return 'MY_PIPELINE_SOURCEPATH'; }

    mapRow(row) {
        return {
            id: Number(row.id) || null,
            name: row.name || null,
            description: row.description || null
        };
    }

    buildInsertQuery(mappedRow) {
        return {
            sql: `INSERT INTO my_table (id, name, description)
                  VALUES ($1, $2, $3)
                  ON CONFLICT (id) DO NOTHING`,
            values: [mappedRow.id, mappedRow.name, mappedRow.description]
        };
    }
}

module.exports = MyPipeline;
```
