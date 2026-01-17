/**
 * Product Lab Revenue Share Pipeline
 * ===================================
 * ETL pipeline for processing product lab revenue share data.
 *
 * Target table: product_lab_rev_share
 * Conflict handling: ON CONFLICT (lab_id, lab_product_id, fee_schedule_name) DO NOTHING
 *
 * Required CSV columns (composite primary key):
 * - lab_id (labid in CSV) - BIGINT
 * - lab_product_id (labproductid in CSV) - TEXT
 * - fee_schedule_name (feeschedulename in CSV) - TEXT
 *
 * Optional CSV columns:
 * - incisive_product_id (incisiveproductid in CSV) - INTEGER
 * - revenue_share (revenueshare in CSV) - FLOAT
 * - commitment_eligible (commitmenteligible in CSV) - BOOLEAN
 *
 * Lab ID Reference: SKDLA=1, Power=2, Biotec=3, Universal=4, 3DDx=5, OB=6
 *
 * Environment variable: PRODUCT_LAB_REV_SHARE_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class ProductLabRevSharePipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'product-lab-rev-share';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'product-lab-rev-share';
    }

    get tableName() {
        return 'product_lab_rev_share';
    }

    get requiredFields() {
        return ['lab_id', 'lab_product_id', 'fee_schedule_name'];
    }

    get envKey() {
        return 'PRODUCT_LAB_REV_SHARE_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     * Handles numeric and boolean conversions
     */
    mapRow(row) {
        // Parse numeric fields
        const labId = row.labid ? Number(row.labid) : null;
        const incisiveProductId = row.incisiveproductid ? Number(row.incisiveproductid) : null;
        const revenueShare = row.revenueshare ? parseFloat(row.revenueshare) : null;

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
            fee_schedule_name: row.feeschedulename || null,
            revenue_share: Number.isNaN(revenueShare) ? null : revenueShare,
            commitment_eligible: commitmentEligible
        };
    }

    /**
     * Build INSERT query with composite key conflict handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO product_lab_rev_share (
                lab_id,
                lab_product_id,
                incisive_product_id,
                fee_schedule_name,
                revenue_share,
                commitment_eligible
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ON CONFLICT (lab_id, lab_product_id, fee_schedule_name) DO NOTHING
        `;

        const values = [
            mappedRow.lab_id,
            mappedRow.lab_product_id,
            mappedRow.incisive_product_id,
            mappedRow.fee_schedule_name,
            mappedRow.revenue_share,
            mappedRow.commitment_eligible
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('ProductLabRevSharePipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('ProductLabRevSharePipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = ProductLabRevSharePipeline;
