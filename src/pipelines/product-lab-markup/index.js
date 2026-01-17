/**
 * Product Lab Markup Pipeline
 * ===========================
 * ETL pipeline for processing product lab markup/pricing data.
 *
 * Target table: product_lab_markup
 * Conflict handling: ON CONFLICT (lab_id, lab_product_id) DO NOTHING
 *
 * Required CSV columns:
 * - lab_id (labid in CSV) - BIGINT
 * - lab_product_id (labproductid in CSV) - TEXT
 *
 * Optional CSV columns:
 * - incisive_product_id (incisiveproductid in CSV) - INTEGER
 * - cost - FLOAT
 * - standard_price (standardprice in CSV) - FLOAT
 * - nf_price (nfprice in CSV) - FLOAT
 * - commitment_eligible (commitmenteligible in CSV) - BOOLEAN
 *
 * Environment variable: PRODUCT_LAB_MARKUP_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class ProductLabMarkupPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'product-lab-markup';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'product-lab-markup';
    }

    get tableName() {
        return 'product_lab_markup';
    }

    get requiredFields() {
        return ['lab_id', 'lab_product_id'];
    }

    get envKey() {
        return 'PRODUCT_LAB_MARKUP_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     * Handles numeric and boolean conversions
     */
    mapRow(row) {
        // Parse numeric fields
        const labId = row.labid ? Number(row.labid) : null;
        const incisiveProductId = row.incisiveproductid ? Number(row.incisiveproductid) : null;
        const cost = row.cost ? parseFloat(row.cost) : null;
        const standardPrice = row.standardprice ? parseFloat(row.standardprice) : null;
        const nfPrice = row.nfprice ? parseFloat(row.nfprice) : null;

        // Parse boolean for commitment_eligible
        let commitmentEligible = null;
        if (row.commitmenteligible !== undefined && row.commitmenteligible !== null && row.commitmenteligible !== '') {
            const val = String(row.commitmenteligible).toLowerCase().trim();
            commitmentEligible = (val === 'true' || val === '1' || val === 'yes');
        }

        return {
            lab_id: Number.isNaN(labId) ? null : labId,
            lab_product_id: row.labproductid || null,
            incisive_product_id: Number.isNaN(incisiveProductId) ? null : incisiveProductId,
            cost: Number.isNaN(cost) ? null : cost,
            standard_price: Number.isNaN(standardPrice) ? null : standardPrice,
            nf_price: Number.isNaN(nfPrice) ? null : nfPrice,
            commitment_eligible: commitmentEligible
        };
    }

    /**
     * Build INSERT query with composite key conflict handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO product_lab_markup (
                lab_id,
                lab_product_id,
                incisive_product_id,
                cost,
                standard_price,
                nf_price,
                commitment_eligible
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
            ON CONFLICT (lab_id, lab_product_id) DO NOTHING
        `;

        const values = [
            mappedRow.lab_id,
            mappedRow.lab_product_id,
            mappedRow.incisive_product_id,
            mappedRow.cost,
            mappedRow.standard_price,
            mappedRow.nf_price,
            mappedRow.commitment_eligible
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('ProductLabMarkupPipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('ProductLabMarkupPipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = ProductLabMarkupPipeline;
