/**
 * ETL Orchestrator
 * ================
 * Central coordinator for running ETL pipelines.
 * Handles file discovery, processing, and error reporting.
 *
 * Features:
 * - Generic processing for any registered pipeline
 * - Error CSV generation and S3 upload
 * - Batch processing of all files for a pipeline
 * - Process all pipelines at once
 *
 * Usage:
 * const orchestrator = new Orchestrator(s3Handler, dbConnection, pipelines, config);
 * await orchestrator.processPipeline('dental-groups');
 * await orchestrator.processAllPipelines();
 */

const fs = require('fs/promises');
const path = require('path');
const logger = require('../utils/logger');
const { getPipelineLogger } = require('../utils/logger');
const { S3Handler } = require('../core');

class Orchestrator {
    /**
     * Initialize the orchestrator
     * @param {S3Handler} s3Handler - S3 operations handler
     * @param {DatabaseConnection} dbConnection - Database connection
     * @param {Object} pipelines - Pipeline registry
     * @param {Object} config - Application configuration
     */
    constructor(s3Handler, dbConnection, pipelines, config) {
        this.s3Handler = s3Handler;
        this.dbConnection = dbConnection;
        this.pipelines = pipelines;
        this.config = config;
    }

    /**
     * Get pipeline-specific logger
     * @param {string} pipelineName - Name of the pipeline
     * @returns {winston.Logger} Logger instance
     */
    getLogger(pipelineName) {
        return getPipelineLogger(pipelineName);
    }

    /**
     * Get S3 paths for a pipeline from environment
     * @param {string} envKey - Environment variable key
     * @returns {{ sourcePath: string, processedPath: string, logsPath: string }}
     */
    getPipelinePaths(envKey) {
        try {
            const basePath = process.env[envKey];
            if (!basePath) {
                logger.warn(`Environment variable ${envKey} is not set`);
            }
            return S3Handler.buildPipelinePaths(basePath);
        } catch (error) {
            logger.error('Error getting pipeline paths');
            throw error;
        }
    }

    /**
     * Convert array of objects to CSV string
     * @param {Object[]} data - Array of objects to convert
     * @returns {string} CSV formatted string
     */
    objectToCSV(data) {
        try {
            if (!data || !data.length) return '';

            const headers = Object.keys(data[0]).join(',');
            const rows = data.map(row =>
                Object.values(row)
                    .map(v => `"${String(v ?? '').replace(/\r?\n/g, ' ').replace(/"/g, '""')}"`)
                    .join(',')
            );

            return [headers, ...rows].join('\n');
        } catch (error) {
            logger.error('Error converting data to CSV');
            throw error;
        }
    }

    /**
     * Create CSV file with error remarks
     * @param {string} fileName - Name of the file
     * @param {string} csvContent - CSV content to write
     * @returns {Promise<string>} Path to created file
     */
    async createRemarkCSV(fileName, csvContent) {
        try {
            const logDir = './logs';
            await fs.mkdir(logDir, { recursive: true });

            const remarkFilePath = path.join(logDir, fileName);
            await fs.writeFile(remarkFilePath, csvContent);

            logger.info('Remark CSV created successfully', { remarkFilePath });
            return remarkFilePath;
        } catch (error) {
            logger.error('Error creating remark CSV');
            throw error;
        }
    }

    /**
     * Process a single file through a pipeline
     * @param {string} pipelineName - Name of the pipeline
     * @param {string} fileName - CSV file name
     * @returns {Promise<Object>} Processing results
     */
    async processFile(pipelineName, fileName) {
        const startTime = Date.now();
        const pipelineLogger = this.getLogger(pipelineName);

        // Get pipeline class
        let PipelineClass;
        try {
            PipelineClass = this.pipelines[pipelineName];
            if (!PipelineClass) {
                pipelineLogger.error('Pipeline not found');
                throw new Error(`Pipeline not found: ${pipelineName}`);
            }
        } catch (error) {
            pipelineLogger.error('Error getting pipeline class');
            throw error;
        }

        // Get S3 paths for this pipeline
        let paths;
        try {
            paths = this.getPipelinePaths(PipelineClass.prototype.envKey || new PipelineClass(null, null, {}).envKey);

            if (!paths.sourcePath) {
                pipelineLogger.error('Environment variable not set');
                throw new Error(`Environment variable not set for pipeline: ${pipelineName}`);
            }
        } catch (error) {
            pipelineLogger.error('Error getting S3 paths');
            throw error;
        }

        pipelineLogger.info('='.repeat(80));
        pipelineLogger.info(`Starting ${pipelineName} file processing`, {
            pipeline: pipelineName,
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // ==================== ENSURE FOLDERS EXIST ====================
            pipelineLogger.info('Ensuring S3 folders exist');
            await this.s3Handler.ensureFolderExists(paths.sourcePath);

            // ==================== STEP 1: CHECK FILE EXISTS ====================
            pipelineLogger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Handler.checkFileExists(fileName, paths.sourcePath);
            if (!exists) {
                pipelineLogger.error('File not found in S3');
                throw new Error(`File not found in S3: ${fileName}`);
            }

            // ==================== STEP 2: RETRIEVE FILE FROM S3 ====================
            pipelineLogger.info('Step 2: Retrieving file from S3', { fileName });
            const stream = await this.s3Handler.getFile(fileName, paths.sourcePath);

            // ==================== STEP 3: CREATE PIPELINE INSTANCE ====================
            pipelineLogger.info('Step 3: Creating pipeline instance');
            const pipelineConfig = {
                ...this.config.processing,
                sourcePath: paths.sourcePath
            };
            const pipeline = new PipelineClass(
                this.dbConnection.getPool(),
                this.s3Handler,
                pipelineConfig
            );

            // ==================== STEP 4: PARSE CSV ====================
            pipelineLogger.info('Step 4: Parsing CSV file', { fileName });
            const rows = await pipeline.parseCSV(stream);
            pipelineLogger.info('Step 4 completed: CSV parsed successfully', { rowCount: rows.length });

            // Check for empty file
            if (rows.length === 0) {
                pipelineLogger.warn('CSV file is empty, skipping processing');
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            // ==================== STEP 5: PROCESS ROWS ====================
            pipelineLogger.info('Step 5: Processing rows and inserting into database', { rowCount: rows.length });
            const processResult = await pipeline.processRows(rows, fileName);
            const { successCount, errorCount, missingFieldErrors, validRows, errorRows } = processResult;
            pipelineLogger.info('Step 5 completed: Rows processed successfully', { successCount, errorCount });

            pipelineLogger.info(`${pipelineName} table populated`, {
                successCount,
                errorCount,
                message: errorCount > 0 ? `${errorCount} rows skipped` : 'All rows valid'
            });

            const duration = Date.now() - startTime;

            // ==================== STEP 6: UPLOAD VALID ROWS TO PROCESSED ====================
            if (validRows.length > 0) {
                pipelineLogger.info('Step 6: Uploading valid rows to processed folder', { validRowCount: validRows.length });
                try {
                    const validCsvContent = this.objectToCSV(validRows);
                    const validFilePath = await this.createRemarkCSV(`valid_${fileName}`, validCsvContent);
                    await this.s3Handler.uploadProcessedFile(validFilePath, fileName, paths.processedPath);
                    pipelineLogger.info('Step 6 completed: Valid rows uploaded to processed folder');
                } catch (error) {
                    pipelineLogger.error('Error uploading valid rows to processed folder');
                }
            } else {
                pipelineLogger.info('Step 6: Skipped - No valid rows to upload');
            }

            // ==================== STEP 7: UPLOAD LOG FILE (all rows with status) ====================
            pipelineLogger.info('Step 7: Uploading log file to S3', { totalRows: rows.length, successCount, errorCount });
            try {
                // Add etl_status to valid rows
                const validRowsWithStatus = validRows.map(row => ({
                    ...row,
                    etl_status: 'success',
                    etl_reason: '',
                    missingFields: ''
                }));

                // Error rows already have reason, rename and add etl_status
                const errorRowsWithStatus = errorRows.map(row => {
                    const { reason, missingFields, ...rest } = row;
                    return {
                        ...rest,
                        etl_status: 'error',
                        etl_reason: reason || '',
                        missingFields: Array.isArray(missingFields) ? missingFields.join(', ') : (missingFields || '')
                    };
                });

                // Combine all rows
                const allRows = [...validRowsWithStatus, ...errorRowsWithStatus];
                const logCsvContent = this.objectToCSV(allRows);
                const logFilePath = await this.createRemarkCSV(`log_${fileName}`, logCsvContent);
                const s3LogKey = await this.s3Handler.uploadLogFile(logFilePath, fileName, paths.logsPath);
                pipelineLogger.info('Step 7 completed: Log file uploaded successfully', { s3LogKey });
            } catch (error) {
                pipelineLogger.error('Error uploading log file to S3');
            }

            // ==================== STEP 8: DELETE ORIGINAL FILE FROM SOURCE ====================
            pipelineLogger.info('Step 8: Deleting original file from source folder');
            try {
                await this.s3Handler.deleteFile(fileName, paths.sourcePath);
                pipelineLogger.info('Step 8 completed: Original file deleted from source');
            } catch (error) {
                pipelineLogger.error('Error deleting original file from source');
            }

            // ==================== PROCESSING COMPLETE ====================
            pipelineLogger.info('File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                duration: `${duration}ms`
            });
            pipelineLogger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString()
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            pipelineLogger.error('File processing failed', {
                fileName,
                error: error.message,
                duration: `${duration}ms`
            });
            pipelineLogger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process all files for a specific pipeline
     * @param {string} pipelineName - Name of the pipeline
     * @returns {Promise<Object>} Batch processing results
     */
    async processPipeline(pipelineName) {
        const pipelineLogger = this.getLogger(pipelineName);

        // Get pipeline class
        let PipelineClass;
        try {
            PipelineClass = this.pipelines[pipelineName];
            if (!PipelineClass) {
                pipelineLogger.error('Pipeline not found');
                throw new Error(`Pipeline not found: ${pipelineName}`);
            }
        } catch (error) {
            pipelineLogger.error('Error getting pipeline class');
            throw error;
        }

        // Get S3 paths
        let paths;
        try {
            const tempPipeline = new PipelineClass(null, null, {});
            paths = this.getPipelinePaths(tempPipeline.envKey);
        } catch (error) {
            pipelineLogger.error('Error getting S3 paths for pipeline');
            throw error;
        }

        if (!paths.sourcePath) {
            pipelineLogger.warn(`Pipeline ${pipelineName} not configured (missing env var)`);
            return { processed: 0, successful: 0, failed: 0, results: [] };
        }

        try {
            await this.s3Handler.ensureFolderExists(paths.sourcePath);
        } catch (error) {
            pipelineLogger.error('Error creating source folder in S3');
            throw error;
        }

        try {
            pipelineLogger.info(`Scanning for ${pipelineName} files in S3`, {
                bucket: this.s3Handler.bucket,
                path: paths.sourcePath
            });

            let files;
            try {
                files = await this.s3Handler.listFiles(paths.sourcePath, paths.processedPath, paths.logsPath);
            } catch (error) {
                pipelineLogger.error('Error listing files in S3');
                throw error;
            }

            if (files.length === 0) {
                pipelineLogger.info(`No ${pipelineName} files found to process`);
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            pipelineLogger.info(`Found ${files.length} ${pipelineName} CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processFile(pipelineName, file);
                    results.push({ file, ...result });
                    if (result.success) {
                        successful++;
                        pipelineLogger.info(`File processed successfully: ${file}`);
                    } else {
                        failed++;
                        pipelineLogger.warn(`File processing returned failure: ${file}`);
                    }
                } catch (error) {
                    pipelineLogger.error(`Error processing file: ${file}`);
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            pipelineLogger.info(`${pipelineName} pipeline completed`, {
                total: files.length,
                successful,
                failed
            });

            return { processed: files.length, successful, failed, results };

        } catch (error) {
            pipelineLogger.error(`Error processing ${pipelineName} pipeline`);
            throw error;
        }
    }

    /**
     * Process all files from all registered pipelines
     * @returns {Promise<Object>} Combined results from all pipelines
     */
    async processAllPipelines() {
        const allResults = {};
        let totalProcessed = 0;
        let totalSuccessful = 0;
        let totalFailed = 0;

        // Get all pipeline names (excluding utility functions)
        let pipelineNames;
        try {
            pipelineNames = Object.keys(this.pipelines).filter(
                key => typeof this.pipelines[key] === 'function' &&
                    this.pipelines[key].pipelineName
            );
        } catch (error) {
            logger.error('Error getting pipeline names');
            throw error;
        }

        logger.info('Processing all pipelines', {
            pipelines: pipelineNames,
            count: pipelineNames.length
        });

        for (const pipelineName of pipelineNames) {
            const pipelineLogger = this.getLogger(pipelineName);

            try {
                pipelineLogger.info(`Starting pipeline: ${pipelineName}`);

                const result = await this.processPipeline(pipelineName);
                allResults[pipelineName] = result;
                totalProcessed += result.processed;
                totalSuccessful += result.successful;
                totalFailed += result.failed;

                pipelineLogger.info(`Pipeline completed: ${pipelineName}`, {
                    processed: result.processed,
                    successful: result.successful,
                    failed: result.failed
                });

            } catch (error) {
                pipelineLogger.error(`Pipeline failed: ${pipelineName}`);
                allResults[pipelineName] = {
                    processed: 0,
                    successful: 0,
                    failed: 0,
                    error: error.message
                };
            }
        }

        logger.info('All pipelines processed', {
            totalFiles: totalProcessed,
            successful: totalSuccessful,
            failed: totalFailed,
            pipelineCount: pipelineNames.length
        });

        return {
            processed: totalProcessed,
            successful: totalSuccessful,
            failed: totalFailed,
            pipelines: allResults
        };
    }

    /**
     * Get list of available pipeline names
     * @returns {string[]} Array of pipeline names
     */
    getAvailablePipelines() {
        try {
            return Object.keys(this.pipelines).filter(
                key => typeof this.pipelines[key] === 'function' &&
                    this.pipelines[key].pipelineName
            );
        } catch (error) {
            logger.error('Error getting available pipelines');
            return [];
        }
    }
}

module.exports = Orchestrator;
