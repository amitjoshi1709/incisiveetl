/**
 * Orders Pipeline
 * ===============
 * ETL pipeline for processing order CSV files into orders_stage table.
 *
 * Special features:
 * - Truncates staging table before processing
 * - Generates row hash for deduplication
 * - Calls merge_orders_stage() stored procedure after insert
 *
 * Required CSV columns:
 * - submissiondate, casedate, caseid, productid, quantity, customerid
 *
 * Environment variable: SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');
const { generateRowHash } = require('../../utils/hash');

class OrdersPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'orders';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'orders';
    }

    get tableName() {
        return 'orders_stage';
    }

    get requiredFields() {
        return ['submissiondate', 'casedate', 'caseid', 'productid', 'quantity', 'customerid'];
    }

    get envKey() {
        return 'SOURCEPATH';
    }

    // ==================== OPTIONAL OVERRIDES ====================

    /**
     * Orders pipeline truncates staging table before each run
     */
    get shouldTruncate() {
        return true;
    }

    /**
     * Transform CSV row to database schema
     */
    mapRow(row) {
        return {
            submissiondate: row.submissiondate || null,
            shippingdate: row.shippingdate || null,
            casedate: row.casedate || null,
            caseid: row.caseid || null,
            productid: row.productid || null,
            productdescription: row.productdescription || null,
            quantity: row.quantity ? parseInt(row.quantity, 10) : null,
            productprice: row.productprice || null,
            patientname: row.patientname || null,
            customerid: row.customerid || null,
            customername: row.customername || null,
            address: row.address || null,
            phonenumber: row.phonenumber || null,
            casestatus: row.casestatus || null,
            holdreason: row.holdreason || null,
            estimatecompletedate: row.estimatecompletedate || null,
            requestedreturndate: row.requestedreturndate || null,
            trackingnumber: row.trackingnumber || null,
            estimatedshipdate: row.estimatedshipdate || null,
            holddate: row.holddate || null,
            deliverystatus: row.deliverystatus || null,
            notes: row.notes || null,
            onhold: row.onhold || null,
            shade: row.shade || null,
            mold: row.mold || null,
            doctorpreferences: row.doctorpreferences || null,
            productpreferences: row.productpreferences || null,
            comments: row.comments || null,
            casetotal: row.casetotal || null
        };
    }

    /**
     * Build INSERT query with hash for deduplication
     */
    buildInsertQuery(mappedRow, fileName = '') {
        const caseid = mappedRow.caseid ? parseInt(mappedRow.caseid, 10) : null;
        const rowHash = generateRowHash(mappedRow);
        const sourceFileKey = this.config.sourcePath + (fileName || '');

        const sql = `
            INSERT INTO orders_stage (
                submissiondate, shippingdate, casedate, caseid, productid,
                productdescription, quantity, productprice, patientname,
                customerid, customername, address, phonenumber, casestatus,
                holdreason, estimatecompletedate, requestedreturndate,
                trackingnumber, estimatedshipdate, holddate, deliverystatus,
                notes, onhold, shade, mold, doctorpreferences,
                productpreferences, comments, casetotal,
                source_file_key, row_hash
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31
            )
        `;

        const values = [
            mappedRow.submissiondate,
            mappedRow.shippingdate,
            mappedRow.casedate,
            caseid,
            mappedRow.productid,
            mappedRow.productdescription,
            mappedRow.quantity,
            mappedRow.productprice,
            mappedRow.patientname,
            mappedRow.customerid,
            mappedRow.customername,
            mappedRow.address,
            mappedRow.phonenumber,
            mappedRow.casestatus,
            mappedRow.holdreason,
            mappedRow.estimatecompletedate,
            mappedRow.requestedreturndate,
            mappedRow.trackingnumber,
            mappedRow.estimatedshipdate,
            mappedRow.holddate,
            mappedRow.deliverystatus,
            mappedRow.notes,
            mappedRow.onhold,
            mappedRow.shade,
            mappedRow.mold,
            mappedRow.doctorpreferences,
            mappedRow.productpreferences,
            mappedRow.comments,
            mappedRow.casetotal,
            sourceFileKey,
            rowHash
        ];

        return { sql, values };
    }

    /**
     * Call merge stored procedure after all rows are inserted
     */
    async postProcess(client) {
        try {
            this.logger.info('OrdersPipeline: Calling merge_orders_stage() stored procedure');
            await client.query('CALL merge_orders_stage()');
            this.logger.info('OrdersPipeline: Stored procedure executed successfully');
        } catch (error) {
            this.logger.error('OrdersPipeline: Error calling stored procedure', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = OrdersPipeline;
