/**
 * Salesforce Extractor
 * ====================
 * Main extractor class for fetching data from Salesforce and uploading to S3.
 *
 * Data Flow:
 * Salesforce API -> Fetch records -> Convert to CSV -> Upload to S3
 *
 * Usage:
 * const extractor = new SalesforceExtractor(config, s3Handler);
 * await extractor.extract('dental-groups');
 */

const SalesforceClient = require('./SalesforceClient');
const dentalGroupsQuery = require('./queries/dental-groups');
const logger = require('../../utils/logger');
const { S3Handler } = require('../../core');

// Query registry - maps extractor names to query configurations
const QUERIES = {
    'dental-groups': dentalGroupsQuery
};

class SalesforceExtractor {
    /**
     * Initialize Salesforce extractor
     * @param {Object} sfConfig - Salesforce configuration
     * @param {S3Handler} s3Handler - S3 operations handler
     */
    constructor(sfConfig, s3Handler) {
        this.client = new SalesforceClient(sfConfig);
        this.s3Handler = s3Handler;
    }

    /**
     * Get list of available extractors
     * @returns {string[]} Array of extractor names
     */
    static getAvailableExtractors() {
        return Object.keys(QUERIES);
    }

    /**
     * Check if an extractor exists
     * @param {string} name - Extractor name
     * @returns {boolean}
     */
    static hasExtractor(name) {
        return !!QUERIES[name];
    }

    /**
     * Convert array of objects to CSV string
     * @param {Object[]} data - Array of objects to convert
     * @returns {string} CSV formatted string
     */
    objectToCSV(data) {
        if (!data || !data.length) return '';

        const headers = Object.keys(data[0]).join(',');
        const rows = data.map(row =>
            Object.values(row)
                .map(v => `"${String(v ?? '').replace(/\r?\n/g, ' ').replace(/"/g, '""')}"`)
                .join(',')
        );

        return [headers, ...rows].join('\n');
    }

    /**
     * Generate timestamped filename
     * @param {string} prefix - Filename prefix
     * @returns {string} Filename with timestamp
     */
    generateFilename(prefix) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        return `${prefix}-${timestamp}.csv`;
    }

    /**
     * Extract data from Salesforce and upload to S3
     * @param {string} extractorName - Name of the extractor to run
     * @returns {Promise<Object>} Extraction results
     */
    async extract(extractorName) {
        const startTime = Date.now();

        // Get query configuration
        const queryConfig = QUERIES[extractorName];
        if (!queryConfig) {
            throw new Error(`Unknown extractor: ${extractorName}. Available: ${Object.keys(QUERIES).join(', ')}`);
        }

        logger.info('='.repeat(80));
        logger.info(`SalesforceExtractor: Starting extraction`, {
            extractor: extractorName,
            startTime: new Date().toISOString()
        });

        try {
            // Step 1: Authenticate to Salesforce
            logger.info('Step 1: Authenticating to Salesforce');
            await this.client.authenticate();
            logger.info('Step 1 completed: Authentication successful');

            // Step 2: Execute SOQL query
            logger.info('Step 2: Executing SOQL query');
            const records = await this.client.query(queryConfig.soql);
            logger.info('Step 2 completed: Query returned records', {
                recordCount: records.length
            });

            if (records.length === 0) {
                logger.warn('SalesforceExtractor: No records found, skipping upload');
                return {
                    success: true,
                    extractor: extractorName,
                    recordCount: 0,
                    skipped: true,
                    duration: Date.now() - startTime
                };
            }

            // Step 3: Map records to CSV format
            logger.info('Step 3: Mapping records to CSV format');
            const csvRows = records.map(record => queryConfig.mapRecord(record));
            logger.info('Step 3 completed: Records mapped', {
                rowCount: csvRows.length
            });

            // Step 4: Convert to CSV string
            logger.info('Step 4: Converting to CSV');
            const csvContent = this.objectToCSV(csvRows);
            const csvBuffer = Buffer.from(csvContent, 'utf8');
            logger.info('Step 4 completed: CSV generated', {
                sizeBytes: csvBuffer.length
            });

            // Step 5: Get S3 destination path
            logger.info('Step 5: Determining S3 destination');
            const basePath = process.env[queryConfig.s3EnvKey];
            if (!basePath) {
                throw new Error(`Environment variable ${queryConfig.s3EnvKey} not set`);
            }
            const paths = S3Handler.buildPipelinePaths(basePath);
            const filename = this.generateFilename(queryConfig.filePrefix);
            const s3Key = paths.sourcePath + filename;
            logger.info('Step 5 completed: S3 path determined', {
                bucket: this.s3Handler.bucket,
                key: s3Key
            });

            // Step 6: Upload to S3
            logger.info('Step 6: Uploading to S3');
            await this.uploadToS3(csvBuffer, s3Key);
            logger.info('Step 6 completed: File uploaded successfully');

            const duration = Date.now() - startTime;
            logger.info('='.repeat(80));
            logger.info('SalesforceExtractor: Extraction completed', {
                extractor: extractorName,
                recordCount: records.length,
                s3Key,
                duration: `${duration}ms`
            });

            return {
                success: true,
                extractor: extractorName,
                recordCount: records.length,
                filename,
                s3Key,
                bucket: this.s3Handler.bucket,
                duration
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('SalesforceExtractor: Extraction failed', {
                extractor: extractorName,
                error: error.message,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Upload CSV buffer to S3
     * @param {Buffer} buffer - CSV content as buffer
     * @param {string} s3Key - S3 object key
     * @returns {Promise<void>}
     */
    async uploadToS3(buffer, s3Key) {
        const { PutObjectCommand } = require('@aws-sdk/client-s3');

        const command = new PutObjectCommand({
            Bucket: this.s3Handler.bucket,
            Key: s3Key,
            Body: buffer,
            ContentType: 'text/csv'
        });

        await this.s3Handler.client.send(command);

        logger.info('SalesforceExtractor: CSV uploaded to S3', {
            key: s3Key,
            bucket: this.s3Handler.bucket,
            size: buffer.length
        });
    }
}

module.exports = SalesforceExtractor;
