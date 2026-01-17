/**
 * Dental Practices Pipeline
 * =========================
 * ETL pipeline for processing dental practices CSV files.
 *
 * Target table: dental_practices
 * Conflict handling: ON CONFLICT (practice_id) DO NOTHING
 *
 * Required CSV columns:
 * - practice_id (practiceid in CSV)
 * - dental_group_id (dentalgroupid in CSV)
 *
 * Environment variable: DENTAL_PRACTICES_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class DentalPracticesPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'dental-practices';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'dental-practices';
    }

    get tableName() {
        return 'dental_practices';
    }

    get requiredFields() {
        return ['practice_id', 'dental_group_id'];
    }

    get envKey() {
        return 'DENTAL_PRACTICES_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     * Maps CSV column names to database column names
     */
    mapRow(row) {
        const practiceId = row.practiceid ? Number(row.practiceid) : null;
        const dentalGroupId = row.dentalgroupid ? Number(row.dentalgroupid) : null;

        return {
            practice_id: Number.isNaN(practiceId) ? null : practiceId,
            dental_group_id: Number.isNaN(dentalGroupId) ? null : dentalGroupId,
            dental_group_name: row.dentalgroupname || null,
            address: row.address || null,
            address_2: row.address2 || null,
            city: row.city || null,
            state: row.state || null,
            zip: row.zip || null,
            phone: row.phone || null,
            clinical_email: row.clinicalemail || null,
            billing_email: row.billingemail || null,
            incisive_email: row.incisiveemail || null,
            preferred_contact_method: row.preferredcontactmethod || null,
            fee_schedule: row.feeschedule || null,
            status: row.status || null
        };
    }

    /**
     * Build INSERT query with ON CONFLICT handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO dental_practices (
                practice_id,
                dental_group_id,
                dental_group_name,
                address,
                address_2,
                city,
                state,
                zip,
                phone,
                clinical_email,
                billing_email,
                incisive_email,
                preferred_contact_method,
                fee_schedule,
                status
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15
            )
            ON CONFLICT (practice_id) DO NOTHING
        `;

        const values = [
            mappedRow.practice_id,
            mappedRow.dental_group_id,
            mappedRow.dental_group_name,
            mappedRow.address,
            mappedRow.address_2,
            mappedRow.city,
            mappedRow.state,
            mappedRow.zip,
            mappedRow.phone,
            mappedRow.clinical_email,
            mappedRow.billing_email,
            mappedRow.incisive_email,
            mappedRow.preferred_contact_method,
            mappedRow.fee_schedule,
            mappedRow.status
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('DentalPracticesPipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('DentalPracticesPipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = DentalPracticesPipeline;
