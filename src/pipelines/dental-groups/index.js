/**
 * Dental Groups Pipeline
 * ======================
 * ETL pipeline for processing dental group organization data.
 *
 * Target table: dental_groups
 * Conflict handling: ON CONFLICT (dental_group_id) DO NOTHING
 *
 * Required CSV columns:
 * - dental_group_id (dentalgroupid in CSV) - BIGINT PRIMARY KEY
 *
 * Optional columns:
 * - name, address, address_2, city, state, zip
 * - account_type, centralized_billing (boolean), sales_channel, sales_rep
 *
 * Environment variable: DENTAL_GROUPS_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class DentalGroupsPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'dental-groups';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'dental-groups';
    }

    get tableName() {
        return 'dental_groups';
    }

    get requiredFields() {
        return ['dental_group_id', 'name'];
    }

    get envKey() {
        return 'DENTAL_GROUPS_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     * Handles boolean conversion for centralized_billing
     */
    mapRow(row) {
        const dentalGroupId = row.dentalgroupid ? Number(row.dentalgroupid) : null;

        // Parse boolean for centralized_billing
        // Accepts: true, TRUE, 1, false, FALSE, 0
        let centralizedBilling = null;
        if (row.centralizedbilling !== undefined && row.centralizedbilling !== null && row.centralizedbilling !== '') {
            const val = String(row.centralizedbilling).toLowerCase().trim();
            centralizedBilling = (val === 'true' || val === '1');
        }

        return {
            dental_group_id: Number.isNaN(dentalGroupId) ? null : dentalGroupId,
            name: row.name || null,
            address: row.address || null,
            address_2: row.address2 || null,
            city: row.city || null,
            state: row.state || null,
            zip: row.zip || null,
            account_type: row.accounttype || null,
            centralized_billing: centralizedBilling,
            sales_channel: row.saleschannel || null,
            sales_rep: row.salesrep || null
        };
    }

    /**
     * Build INSERT query with ON CONFLICT handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO dental_groups (
                dental_group_id,
                name,
                address,
                address_2,
                city,
                state,
                zip,
                account_type,
                centralized_billing,
                sales_channel,
                sales_rep
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11
            )
            ON CONFLICT (dental_group_id) DO NOTHING
        `;

        const values = [
            mappedRow.dental_group_id,
            mappedRow.name,
            mappedRow.address,
            mappedRow.address_2,
            mappedRow.city,
            mappedRow.state,
            mappedRow.zip,
            mappedRow.account_type,
            mappedRow.centralized_billing,
            mappedRow.sales_channel,
            mappedRow.sales_rep
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('DentalGroupsPipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('DentalGroupsPipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = DentalGroupsPipeline;
