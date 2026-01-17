/**
 * Product Catalog Pipeline
 * ========================
 * ETL pipeline for processing product catalog CSV files.
 *
 * Target table: incisive_product_catalog
 * Conflict handling: ON CONFLICT (incisive_id) DO NOTHING
 *
 * Required CSV columns:
 * - incisive_id (incisiveid in CSV)
 * - incisive_name (incisivename in CSV)
 * - category
 *
 * Environment variable: PRODUCT_CATALOG_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class ProductCatalogPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'product-catalog';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'product-catalog';
    }

    get tableName() {
        return 'incisive_product_catalog';
    }

    get requiredFields() {
        return ['incisive_id', 'incisive_name', 'category'];
    }

    get envKey() {
        return 'PRODUCT_CATALOG_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     * Handles CSV column name variations (incisiveid -> incisive_id)
     */
    mapRow(row) {
        const incisiveId = row.incisiveid ? Number(row.incisiveid) : null;

        return {
            incisive_id: Number.isNaN(incisiveId) ? null : incisiveId,
            incisive_name: row.incisivename || null,
            category: row.category || null,
            sub_category: row.subcategory || null
        };
    }

    /**
     * Build INSERT query with ON CONFLICT handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO incisive_product_catalog (
                incisive_id, incisive_name, category, sub_category
            ) VALUES (
                $1, $2, $3, $4
            )
            ON CONFLICT (incisive_id) DO NOTHING
        `;

        const values = [
            mappedRow.incisive_id,
            mappedRow.incisive_name,
            mappedRow.category,
            mappedRow.sub_category
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('ProductCatalogPipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('ProductCatalogPipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = ProductCatalogPipeline;
