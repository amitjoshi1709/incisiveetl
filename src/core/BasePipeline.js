/**
 * BasePipeline.js
 * ===============
 * Abstract base class that all ETL pipelines extend.
 * Provides common CSV processing, validation, and database transaction handling.
 *
 * To create a new pipeline:
 * 1. Create a new folder in src/pipelines/<pipeline-name>/
 * 2. Extend this class and implement required methods
 * 3. Export pipeline config from index.js
 *
 * @example
 * class MyPipeline extends BasePipeline {
 *     get name() { return 'my-pipeline'; }
 *     get tableName() { return 'my_table'; }
 *     get requiredFields() { return ['id', 'name']; }
 *     mapRow(row) { return { id: row.id, name: row.name }; }
 *     buildInsertQuery(row) { return { sql: '...', values: [...] }; }
 * }
 */

const csv = require('csv-parser');
const { Readable } = require('stream');
const { getPipelineLogger } = require('../utils/logger');

class BasePipeline {
    /**
     * @param {Object} dbPool - PostgreSQL connection pool
     * @param {Object} s3Handler - S3 handler instance
     * @param {Object} config - Pipeline-specific configuration
     */
    constructor(dbPool, s3Handler, config = {}) {
        this.dbPool = dbPool;
        this.s3Handler = s3Handler;
        this.config = config;
        this.batchSize = config.batchSize || 100;
        this._logger = null;
    }

    /**
     * Get pipeline-specific logger (creates separate log file for this pipeline)
     * @returns {winston.Logger} Logger instance
     */
    get logger() {
        if (!this._logger) {
            this._logger = getPipelineLogger(this.name);
        }
        return this._logger;
    }

    // ==================== ABSTRACT METHODS (Must be implemented) ====================

    /**
     * Pipeline identifier used for CLI commands and logging
     * @returns {string} Pipeline name (e.g., 'dental-groups')
     */
    get name() {
        throw new Error('Pipeline must implement "name" getter');
    }

    /**
     * Target database table name
     * @returns {string} Table name
     */
    get tableName() {
        throw new Error('Pipeline must implement "tableName" getter');
    }

    /**
     * Fields that must be present and non-empty for row to be valid
     * @returns {string[]} Array of required field names
     */
    get requiredFields() {
        throw new Error('Pipeline must implement "requiredFields" getter');
    }

    /**
     * Environment variable key for S3 source path
     * @returns {string} Environment variable name
     */
    get envKey() {
        throw new Error('Pipeline must implement "envKey" getter');
    }

    /**
     * Transform normalized CSV row to database schema
     * @param {Object} normalizedRow - Row with lowercase keys, no special chars
     * @returns {Object} Mapped row ready for database insertion
     */
    mapRow(normalizedRow) {
        throw new Error('Pipeline must implement "mapRow" method');
    }

    /**
     * Build INSERT query for a single row
     * @param {Object} mappedRow - Row returned from mapRow()
     * @returns {{ sql: string, values: any[] }} SQL query and parameter values
     */
    buildInsertQuery(mappedRow) {
        throw new Error('Pipeline must implement "buildInsertQuery" method');
    }

    // ==================== OPTIONAL OVERRIDES ====================

    /**
     * Whether to truncate table before processing (default: false)
     * @returns {boolean}
     */
    get shouldTruncate() {
        return false;
    }

    /**
     * Post-processing hook called after all rows are inserted
     * Override to call stored procedures, update stats, etc.
     * @param {Object} client - Database client within transaction
     */
    async postProcess(client) {
        // Default: no post-processing
    }

    /**
     * Custom truncate implementation (override if needed)
     * @param {Object} client - Database client
     */
    async truncateTable(client) {
        await client.query(`TRUNCATE TABLE ${this.tableName}`);
        this.logger.info(`Table ${this.tableName} truncated`);
    }

    // ==================== CORE PROCESSING LOGIC ====================

    /**
     * Parse CSV buffer/stream into array of row objects
     * @param {Buffer|Stream} stream - CSV data
     * @returns {Promise<Object[]>} Parsed rows
     */
    async parseCSV(stream) {
        return new Promise((resolve, reject) => {
            const rows = [];
            const readable = Readable.from(stream);
            readable
                .pipe(csv())
                .on('data', (row) => rows.push(row))
                .on('end', () => resolve(rows))
                .on('error', reject);
        });
    }

    /**
     * Normalize CSV row - lowercase keys, remove special characters
     * @param {Object} row - Raw CSV row
     * @returns {Object} Normalized row
     */
    normalizeRow(row) {
        const normalized = {};
        for (const [key, value] of Object.entries(row)) {
            const normalizedKey = key
                .toLowerCase()
                .trim()
                .replace(/[^a-z0-9]/g, '');
            normalized[normalizedKey] = value;
        }
        return normalized;
    }

    /**
     * Validate that all required fields are present and non-empty
     * @param {Object} mappedRow - Row after mapping
     * @returns {string[]} Array of missing field names (empty if valid)
     */
    validateRow(mappedRow) {
        return this.requiredFields.filter(
            key => mappedRow[key] === null ||
                   mappedRow[key] === undefined ||
                   mappedRow[key] === ''
        );
    }

    /**
     * Insert a single row into the database
     * @param {Object} client - Database client
     * @param {Object} mappedRow - Row to insert
     * @returns {Promise<{ success: boolean, errorRow?: Object }>}
     */
    async insertRow(client, mappedRow) {
        const { sql, values } = this.buildInsertQuery(mappedRow);

        try {
            // Use SAVEPOINT to prevent single row error from aborting entire transaction
            await client.query('SAVEPOINT insert_row');
            await client.query(sql, values);
            await client.query('RELEASE SAVEPOINT insert_row');
            return { success: true };
        } catch (error) {
            // Rollback to savepoint to recover transaction state
            await client.query('ROLLBACK TO SAVEPOINT insert_row');
            return {
                success: false,
                errorRow: {
                    row: mappedRow,
                    error_code: error.code,
                    error_message: error.message
                }
            };
        }
    }

    /**
     * Process all rows from CSV with transaction handling
     * @param {Object[]} rows - Parsed CSV rows
     * @param {string} fileName - Source file name for logging
     * @returns {Promise<Object>} Processing results
     */
    async processRows(rows, fileName) {
        const client = await this.dbPool.connect();
        let successCount = 0;
        let errorCount = 0;
        const missingFieldErrors = [];
        const validRows = [];      // Only valid/successful rows
        const errorRows = [];      // Only error rows

        try {
            await client.query('BEGIN');

            // Truncate if required by pipeline
            if (this.shouldTruncate) {
                await this.truncateTable(client);
            }

            // Process in batches
            for (let i = 0; i < rows.length; i += this.batchSize) {
                const batch = rows.slice(i, i + this.batchSize);

                for (let j = 0; j < batch.length; j++) {
                    const row = batch[j];
                    const rowNumber = i + j + 1;

                    const normalizedRow = this.normalizeRow(row);
                    const mappedRow = this.mapRow(normalizedRow);

                    try {
                        const missingFields = this.validateRow(mappedRow);

                        if (missingFields.length > 0) {
                            // Validation failed - missing required fields
                            errorCount++;
                            const errorDetail = {
                                ...row,
                                reason: `Missing required fields: ${missingFields.join(', ')}`,
                                missingFields
                            };
                            missingFieldErrors.push(errorDetail);
                            errorRows.push(errorDetail);

                            this.logger.error('Skipping row with missing fields', {
                                pipeline: this.name,
                                rowNumber,
                                reason: errorDetail.reason,
                                fileName
                            });
                        } else {
                            // Validation passed - insert row
                            const result = await this.insertRow(client, mappedRow);

                            if (result.success) {
                                validRows.push(row);  // Store original row for CSV
                                successCount++;
                                this.logger.debug('Row inserted successfully', {
                                    pipeline: this.name,
                                    fileName,
                                    rowNumber
                                });
                            } else {
                                errorCount++;
                                const errorDetail = {
                                    ...row,
                                    reason: result?.errorRow?.error_message || 'Insert failed',
                                    missingFields: []
                                };
                                errorRows.push(errorDetail);
                                missingFieldErrors.push(errorDetail);
                                this.logger.error('Error inserting row', {
                                    pipeline: this.name,
                                    fileName,
                                    rowNumber,
                                    reason: errorDetail.reason
                                });
                            }
                        }
                    } catch (error) {
                        errorCount++;
                        const errorDetail = {
                            ...row,
                            reason: error.message,
                            missingFields: []
                        };
                        missingFieldErrors.push(errorDetail);
                        errorRows.push(errorDetail);

                        this.logger.error('Error processing row', {
                            pipeline: this.name,
                            rowNumber,
                            error: error.message,
                            fileName
                        });
                    }
                }

                this.logger.info('Batch processed', {
                    pipeline: this.name,
                    batchStart: i + 1,
                    batchEnd: Math.min(i + this.batchSize, rows.length),
                    total: rows.length,
                    successCount,
                    errorCount
                });
            }

            // Run post-processing (e.g., stored procedures)
            await this.postProcess(client);

            await client.query('COMMIT');
            this.logger.info('Transaction committed', {
                pipeline: this.name,
                fileName,
                totalRows: rows.length,
                successCount,
                errorCount
            });

        } catch (error) {
            await client.query('ROLLBACK');
            this.logger.error('Transaction rolled back', {
                pipeline: this.name,
                error: error.message,
                fileName
            });
            throw error;
        } finally {
            client.release();
        }

        return { successCount, errorCount, missingFieldErrors, validRows, errorRows };
    }
}

module.exports = BasePipeline;
