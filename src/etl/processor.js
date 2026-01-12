const logger = require('../utils/logger');
const fs = require('fs/promises');
const path = require('path');
const { objectToCSV } = require('../utils/csv-helpers');

class ETLProcessor {
    constructor(s3Service, dbService, csvProcessor) {
        this.s3Service = s3Service;
        this.dbService = dbService;
        this.csvProcessor = csvProcessor;
    }

    /**
     * Generate detailed summary report for log file
     * NEW METHOD
     */
    generateSummaryReport(fileName, results) {
        const { rowCount, successCount, errorCount, missingFieldErrors, duration } = results;

        let report = '\n' + '='.repeat(80) + '\n';
        report += 'ETL PROCESSING SUMMARY REPORT\n';
        report += '='.repeat(80) + '\n\n';
        report += `File Name: ${fileName}\n`;
        report += `Processing Date: ${new Date().toISOString()}\n`;
        report += `Duration: ${(duration / 1000).toFixed(2)} seconds\n\n`;

        report += 'STATISTICS:\n';
        report += '-'.repeat(80) + '\n';
        report += `Total Rows in CSV: ${rowCount}\n`;
        report += `Successfully Processed: ${successCount}\n`;
        report += `Failed/Skipped: ${errorCount}\n`;
        report += `Success Rate: ${rowCount > 0 ? ((successCount / rowCount) * 100).toFixed(2) : 0}%\n\n`;

        if (missingFieldErrors && missingFieldErrors.length > 0) {
            report += 'MISSING FIELD DETAILS:\n';
            report += '-'.repeat(80) + '\n';

            // Group by missing field type
            const groupedByField = missingFieldErrors.reduce((acc, err) => {
                const field = err.missingField;
                if (!acc[field]) acc[field] = [];
                acc[field].push(err);
                return acc;
            }, {});

            for (const [field, errors] of Object.entries(groupedByField)) {
                report += `\n${field.toUpperCase()} - ${errors.length} occurrence(s):\n`;
                report += '  ' + '-'.repeat(76) + '\n';

                errors.forEach((err, idx) => {
                    report += `  ${idx + 1}. Row #${err.rowNumber}\n`;
                    report += `     Case ID: ${err.caseid}\n`;
                    report += `     Product ID: ${err.productid}\n`;
                    report += `     Reason: ${err.reason}\n\n`;
                });
            }
        } else {
            report += 'MISSING FIELD DETAILS:\n';
            report += '-'.repeat(80) + '\n';
            report += 'No missing fields detected. All rows were valid.\n\n';
        }

        report += '='.repeat(80) + '\n';
        report += 'END OF REPORT\n';
        report += '='.repeat(80) + '\n';

        return report;
    }

    /**
     * Append summary to log file
     */
    async appendSummaryToLog(fileName, results) {
        try {
            const logFilePath = path.join('./logs', 'combined.log');
            const summary = this.generateSummaryReport(fileName, results);

            await fs.appendFile(logFilePath, summary);
            logger.info('Summary report appended to log file', { logFilePath });

            return logFilePath;
        } catch (error) {
            logger.error('Error appending summary to log', { error: error.message });
            throw error;
        }
    }


    /**
     * Create CSV file with remark
     */
    async createRemarkCSV(fileName, results) {
        try {
            const remarkCSVFilePath = path.join('./logs', fileName);
            // const summary = this.generateSummaryReport(fileName, results);

            await fs.writeFile(remarkCSVFilePath, results);
            // logger.info('Summary report appended to log file', { remarkCSVFilePath });

            return remarkCSVFilePath;
        } catch (error) {
            logger.error('Error appending summary to log', { error: error.message });
            throw error;
        }
    }

    /**
     * Process a single file
     * MODIFIED: Added summary generation and S3 upload
     */
    async processFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            // 3. Process rows
            logger.info('Step 3: Inserting rows into orders_stage', {
                fileName,
                rowCount: rows.length
            });
            logger.info('Validating rows (caseid and productid are required)', { fileName });

            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processRows(rows, fileName);

            logger.info('Stage table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped due to missing caseid/productid`
                    : 'All rows valid'
            });

            // 4. Call merge procedure
            logger.info('Step 4: Calling merge_orders_stage() stored procedure', { fileName });
            await this.dbService.callMergeProcedure();
            logger.info('Merge completed successfully');

            // 5. Move file to processed
            logger.info('Step 5: Moving file to processed folder', { fileName });
            await this.s3Service.moveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;

            
            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };
            
            // NEW: 6. Generate and append summary to log
            logger.info('Step 6: Generating summary report', { fileName });
            const logFilePath = await this.appendSummaryToLog(fileName, results);
            
            
            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark)
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // NEW: 7. Upload log file to S3
                logger.info('Step 7: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.uploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
            }
            logger.info('Log file uploaded successfully', { s3LogKey });

            logger.info('✓ File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                skippedReason: errorCount > 0 ? 'Missing caseid or productid' : 'None',
                duration: `${duration}ms`,
                durationSeconds: `${(duration / 1000).toFixed(2)}s`,
                // logFileS3: s3LogKey
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString(),
                // logFileS3: s3LogKey
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('✗ File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process all files in S3 folder
     * MODIFIED: Updated to handle new return structure
     */
    async processAllFiles() {
        try {
            logger.info('Scanning for files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.sourcePath
            });

            const files = await this.s3Service.listFiles();

            if (files.length === 0) {
                logger.info('No files found to process');
                return { processed: 0, files: [] };
            }

            logger.info(`Found ${files.length} CSV file(s) to process`, { files });

            const results = [];
            for (const file of files) {
                try {
                    const result = await this.processFile(file);
                    results.push({ file, ...result });
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                }
            }

            const successful = results.filter(r => r.success).length;
            const failed = results.filter(r => !r.success).length;

            logger.info('All files processed', {
                total: files.length,
                successful,
                failed,
                results
            });

            return { processed: files.length, successful, failed, results };

        } catch (error) {
            logger.error('Error processing files', { error: error.message });
            throw error;
        }
    }
}

module.exports = ETLProcessor;