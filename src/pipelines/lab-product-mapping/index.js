/**
 * Lab Product Mapping Pipeline
 * ============================
 * ETL pipeline for mapping lab products to incisive products.
 *
 * Target table: lab_product_mapping
 * Conflict handling: ON CONFLICT (lab_id, lab_product_id) DO NOTHING
 *
 * Required CSV columns:
 * - lab_id (labid in CSV)
 * - lab_product_id (labproductid in CSV)
 * - incisive_product_id (incisiveproductid in CSV)
 *
 * Environment variable: LAB_PRODUCT_MAPPING_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class LabProductMappingPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'lab-product-mapping';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'lab-product-mapping';
    }

    get tableName() {
        return 'lab_product_mapping';
    }

    get requiredFields() {
        return ['lab_id', 'lab_product_id', 'incisive_product_id'];
    }

    get envKey() {
        return 'LAB_PRODUCT_MAPPING_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     */
    mapRow(row) {
        const labId = row.labid ? Number(row.labid) : null;
        const incisiveProductId = row.incisiveproductid ? Number(row.incisiveproductid) : null;

        return {
            lab_id: Number.isNaN(labId) ? null : labId,
            lab_product_id: row.labproductid || null,
            incisive_product_id: Number.isNaN(incisiveProductId) ? null : incisiveProductId
        };
    }

    /**
     * Build INSERT query with composite key conflict handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO lab_product_mapping (
                lab_id,
                lab_product_id,
                incisive_product_id
            ) VALUES (
                $1, $2, $3
            )
            ON CONFLICT (lab_id, lab_product_id) DO NOTHING
        `;

        const values = [
            mappedRow.lab_id,
            mappedRow.lab_product_id,
            mappedRow.incisive_product_id
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('LabProductMappingPipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('LabProductMappingPipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = LabProductMappingPipeline;
