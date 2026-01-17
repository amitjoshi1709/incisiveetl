/**
 * Pipeline Template
 * =================
 * Copy this folder to create a new pipeline.
 *
 * Steps to create a new pipeline:
 * 1. Copy this folder: cp -r _template my-new-pipeline
 * 2. Rename the class to match your pipeline (e.g., MyNewPipeline)
 * 3. Update static pipelineName to match folder name
 * 4. Implement all required getters and methods
 * 5. Add environment variable to .env file
 * 6. Restart the application - pipeline auto-registers!
 *
 * Required implementations:
 * - name: Pipeline identifier (should match folder name)
 * - tableName: Target database table
 * - requiredFields: Array of mandatory field names
 * - envKey: Environment variable for S3 source path
 * - mapRow(): Transform CSV row to database schema
 * - buildInsertQuery(): Generate INSERT SQL and values
 *
 * Optional overrides:
 * - shouldTruncate: Set to true to truncate table before processing
 * - postProcess(): Called after all rows inserted (e.g., stored procedures)
 * - truncateTable(): Custom truncate implementation
 */

const BasePipeline = require('../../core/BasePipeline');

class TemplatePipeline extends BasePipeline {
    /**
     * IMPORTANT: This must match the folder name for auto-registration
     * Change 'template' to your pipeline name (e.g., 'my-new-pipeline')
     */
    static pipelineName = 'template';

    // ==================== REQUIRED: Update these values ====================

    /**
     * Pipeline name used for CLI commands and logging
     * Should match folder name and static pipelineName
     */
    get name() {
        return 'template';
    }

    /**
     * Target database table name
     */
    get tableName() {
        return 'your_table_name';
    }

    /**
     * Fields that must be present and non-empty
     * Rows missing these fields will be skipped with error
     */
    get requiredFields() {
        return ['field1', 'field2'];
    }

    /**
     * Environment variable containing S3 source path
     * Add this to your .env file: YOUR_PIPELINE_SOURCEPATH=path/to/files
     */
    get envKey() {
        return 'YOUR_PIPELINE_SOURCEPATH';
    }

    // ==================== REQUIRED: Implement these methods ====================

    /**
     * Transform normalized CSV row to database schema
     *
     * @param {Object} row - Row with lowercase keys, no special chars
     *                       e.g., 'MyField' becomes 'myfield'
     * @returns {Object} Mapped row matching database columns
     *
     * Tips:
     * - Use Number() for numeric fields, check isNaN()
     * - Handle boolean conversions explicitly
     * - Return null for missing optional fields
     */
    mapRow(row) {
        // Example numeric field with validation
        const numericField = row.numericfield ? Number(row.numericfield) : null;

        // Example boolean field
        let booleanField = null;
        if (row.booleanfield !== undefined && row.booleanfield !== '') {
            booleanField = ['true', '1', 'yes'].includes(
                String(row.booleanfield).toLowerCase()
            );
        }

        return {
            // Map CSV columns to database columns
            field1: Number.isNaN(numericField) ? null : numericField,
            field2: row.field2 || null,
            boolean_field: booleanField,
            optional_field: row.optionalfield || null
        };
    }

    /**
     * Build INSERT query for database
     *
     * @param {Object} mappedRow - Row returned from mapRow()
     * @returns {{ sql: string, values: any[] }} SQL and parameter values
     *
     * Tips:
     * - Use ON CONFLICT for upsert behavior
     * - Use $1, $2, etc. for parameterized queries
     * - Match values array order to placeholder order
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO your_table_name (
                field1,
                field2,
                boolean_field,
                optional_field
            ) VALUES (
                $1, $2, $3, $4
            )
            ON CONFLICT (field1) DO NOTHING
        `;

        const values = [
            mappedRow.field1,
            mappedRow.field2,
            mappedRow.boolean_field,
            mappedRow.optional_field
        ];

        return { sql, values };
    }

    // ==================== OPTIONAL: Override if needed ====================

    /**
     * Set to true to truncate table before each run
     * Useful for staging tables that get refreshed completely
     */
    // get shouldTruncate() {
    //     return false;
    // }

    /**
     * Called after all rows are inserted (within same transaction)
     * Use for stored procedures, view refreshes, etc.
     *
     * @param {Object} client - Database client in transaction
     */
    // async postProcess(client) {
    //     await client.query('CALL your_stored_procedure()');
    // }
}

module.exports = TemplatePipeline;
