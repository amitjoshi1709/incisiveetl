/**
 * MagicTouch Extractor
 * ====================
 * Extractor for fetching orders from MagicTouch and uploading to S3.
 *
 * Data Flow:
 * MagicTouch API -> Fetch cases & customers -> Flatten by product -> CSV -> S3
 *
 * Environment variables:
 * - MAGICTOUCH_BASE_URL: API base URL
 * - EXPORT_MODE: 'INC' (incremental, last 7 days) or 'FULL' (all cases)
 * - Secrets: MAGICTOUCH_USER_ID, MAGICTOUCH_PASSWORD
 *
 * Usage:
 * const extractor = new MagicTouchExtractor(config, s3Handler);
 * await extractor.extract();
 */

const MagicTouchClient = require('./MagicTouchClient');
const logger = require('../../utils/logger');
const { S3Handler } = require('../../core');

class MagicTouchExtractor {
    /**
     * Initialize MagicTouch extractor
     * @param {Object} config - MagicTouch configuration
     * @param {S3Handler} s3Handler - S3 operations handler
     */
    constructor(config, s3Handler) {
        this.client = new MagicTouchClient(config);
        this.s3Handler = s3Handler;
        this.customerType = 'Incisive';
    }

    /**
     * Get list of available extractors
     * @returns {string[]} Array of extractor names
     */
    static getAvailableExtractors() {
        return ['orders'];
    }

    /**
     * Check if an extractor exists
     * @param {string} name - Extractor name
     * @returns {boolean}
     */
    static hasExtractor(name) {
        return name === 'orders';
    }

    /**
     * Format date to YYYY-MM-DD
     * @param {string} dateStr - Date string
     * @returns {string} Formatted date
     */
    formatDate(dateStr) {
        if (!dateStr) return '';
        try {
            const d = new Date(dateStr);
            return d.toISOString().split('T')[0];
        } catch (e) {
            return dateStr;
        }
    }

    /**
     * Escape value for CSV
     * @param {*} value - Value to escape
     * @returns {string} Escaped value
     */
    escapeCSV(value) {
        if (value === null || value === undefined) {
            return '';
        }
        const str = String(value).replace(/[\r\n]+/g, ' ');
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
    }

    /**
     * Map cases to CSV rows (flattened by product)
     * @param {Object[]} cases - Case records
     * @param {Object} customers - Customer lookup map
     * @returns {Object[]} Array of row objects
     */
    mapCasesToRows(cases, customers) {
        const rows = [];

        for (const c of cases) {
            const customerInfo = customers[c.customerID] || {};

            // Build address
            const address = [c.shipAddress1, c.shipCity, c.shipState, c.shipZipCode]
                .filter(x => x)
                .join(', ');

            // Base case data (matching Orders ETL expected columns)
            const baseRow = {
                labid: 2,  // Hardcoded lab_id for MagicTouch orders
                submissiondate: this.formatDate(c.submissionDate),
                shippingdate: this.formatDate(c.shipDate),
                casedate: this.formatDate(c.dateIn),
                caseid: c.caseNumber,  // Map caseNumber to caseid for Orders ETL
                patientname: [c.patientFirst, c.patientLast].filter(x => x).join(' '),
                customerid: c.customerID,
                customername: c.doctorName || customerInfo.customerName || '',
                address: address,
                phonenumber: customerInfo.officePhone || '',
                casestatus: c.status,
                holdreason: c.holdReason || '',
                estimatecompletedate: this.formatDate(c.estimatedDeliveryDate),
                requestedreturndate: this.formatDate(c.dueDate),
                trackingnumber: c.trackingNumber || '',
                estimatedshipdate: this.formatDate(c.dueDate),
                holddate: this.formatDate(c.holdDate),
                deliverystatus: '',
                notes: c.workOrderNotes || '',
                onhold: c.status === 'On Hold' ? 'Yes' : 'No',
                shade: c.shade || '',
                mold: c.mold || '',
                doctorpreferences: c.customerPreferences || '',
                productpreferences: c.productPreferences || '',
                comments: c.webComments || '',
                casetotal: c.totalCharge || 0
            };

            // Flatten products - one row per product
            const products = c.caseProducts || [];

            if (products.length === 0) {
                rows.push({
                    ...baseRow,
                    productid: '',
                    productdescription: '',
                    quantity: '',
                    productprice: ''
                });
            } else {
                for (const p of products) {
                    rows.push({
                        ...baseRow,
                        productid: p.productID || '',
                        productdescription: p.invoiceDescription || '',
                        quantity: p.quantity || 0,
                        productprice: p.unitPrice || 0
                    });
                }
            }
        }

        return rows;
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
                .map(v => this.escapeCSV(v))
                .join(',')
        );

        return [headers, ...rows].join('\n');
    }

    /**
     * Generate timestamped filename
     * @returns {string} Filename with timestamp
     */
    generateFilename() {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        return `magictouch-orders-${timestamp}.csv`;
    }

    /**
     * Extract orders from MagicTouch and upload to S3
     * @returns {Promise<Object>} Extraction results
     */
    async extract() {
        const startTime = Date.now();

        logger.info('='.repeat(80));
        logger.info('MagicTouchExtractor: Starting extraction', {
            startTime: new Date().toISOString(),
            exportMode: this.client.exportMode
        });

        try {
            // Step 1: Authenticate
            logger.info('Step 1: Authenticating to MagicTouch');
            await this.client.authenticate();
            logger.info('Step 1 completed: Authentication successful');

            // Step 2: Fetch cases
            logger.info('Step 2: Fetching cases');
            const cases = await this.client.fetchCases(this.customerType);
            logger.info('Step 2 completed: Cases fetched', { caseCount: cases.length });

            if (cases.length === 0) {
                logger.warn('MagicTouchExtractor: No cases found, skipping upload');
                return {
                    success: true,
                    extractor: 'magictouch-orders',
                    recordCount: 0,
                    skipped: true,
                    duration: Date.now() - startTime
                };
            }

            // Step 3: Fetch customers for phone lookup
            logger.info('Step 3: Fetching customers for phone lookup');
            const customers = await this.client.fetchCustomers(this.customerType);
            logger.info('Step 3 completed: Customers fetched', { customerCount: Object.keys(customers).length });

            // Step 4: Map cases to CSV rows
            logger.info('Step 4: Mapping cases to CSV rows');
            const csvRows = this.mapCasesToRows(cases, customers);
            logger.info('Step 4 completed: Rows mapped', {
                caseCount: cases.length,
                rowCount: csvRows.length
            });

            // Step 5: Convert to CSV
            logger.info('Step 5: Converting to CSV');
            const csvContent = this.objectToCSV(csvRows);
            const csvBuffer = Buffer.from(csvContent, 'utf8');
            logger.info('Step 5 completed: CSV generated', { sizeBytes: csvBuffer.length });

            // Step 6: Get S3 destination path
            logger.info('Step 6: Determining S3 destination');
            const basePath = process.env.SOURCEPATH;  // Same as Orders ETL
            if (!basePath) {
                throw new Error('Environment variable SOURCEPATH not set');
            }
            const paths = S3Handler.buildPipelinePaths(basePath);
            const filename = this.generateFilename();
            const s3Key = paths.sourcePath + filename;
            logger.info('Step 6 completed: S3 path determined', {
                bucket: this.s3Handler.bucket,
                key: s3Key
            });

            // Step 7: Upload to S3
            logger.info('Step 7: Uploading to S3');
            await this.uploadToS3(csvBuffer, s3Key);
            logger.info('Step 7 completed: File uploaded successfully');

            const duration = Date.now() - startTime;
            logger.info('='.repeat(80));
            logger.info('MagicTouchExtractor: Extraction completed', {
                caseCount: cases.length,
                rowCount: csvRows.length,
                s3Key,
                duration: `${duration}ms`
            });

            return {
                success: true,
                extractor: 'magictouch-orders',
                caseCount: cases.length,
                recordCount: csvRows.length,
                filename,
                s3Key,
                bucket: this.s3Handler.bucket,
                duration
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('MagicTouchExtractor: Extraction failed', {
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

        logger.info('MagicTouchExtractor: CSV uploaded to S3', {
            key: s3Key,
            bucket: this.s3Handler.bucket,
            size: buffer.length
        });
    }
}

module.exports = MagicTouchExtractor;
