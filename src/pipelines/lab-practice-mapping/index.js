/**
 * Lab Practice Mapping Pipeline
 * =============================
 * ETL pipeline for mapping labs to practices.
 *
 * Target table: lab_practice_mapping
 * Conflict handling: ON CONFLICT (lab_id, practice_id) DO NOTHING
 *
 * Required CSV columns:
 * - lab_id (labid in CSV)
 * - practice_id (practiceid in CSV)
 * - lab_practice_id (labpracticeid in CSV)
 *
 * Environment variable: LAB_PRACTICE_MAPPING_SOURCEPATH
 */

const BasePipeline = require('../../core/BasePipeline');

class LabPracticeMappingPipeline extends BasePipeline {
    /**
     * Static pipeline identifier for registry
     */
    static pipelineName = 'lab-practice-mapping';

    // ==================== REQUIRED IMPLEMENTATIONS ====================

    get name() {
        return 'lab-practice-mapping';
    }

    get tableName() {
        return 'lab_practice_mapping';
    }

    get requiredFields() {
        return ['lab_id', 'practice_id', 'lab_practice_id'];
    }

    get envKey() {
        return 'LAB_PRACTICE_MAPPING_SOURCEPATH';
    }

    /**
     * Transform CSV row to database schema
     */
    mapRow(row) {
        const labId = row.labid ? Number(row.labid) : null;
        const practiceId = row.practiceid ? Number(row.practiceid) : null;

        return {
            lab_id: Number.isNaN(labId) ? null : labId,
            practice_id: Number.isNaN(practiceId) ? null : practiceId,
            lab_practice_id: row.labpracticeid || null
        };
    }

    /**
     * Build INSERT query with composite key conflict handling
     */
    buildInsertQuery(mappedRow) {
        const sql = `
            INSERT INTO lab_practice_mapping (
                lab_id,
                practice_id,
                lab_practice_id
            ) VALUES (
                $1, $2, $3
            )
            ON CONFLICT (lab_id, practice_id) DO NOTHING
        `;

        const values = [
            mappedRow.lab_id,
            mappedRow.practice_id,
            mappedRow.lab_practice_id
        ];

        return { sql, values };
    }

    /**
     * Post-processing hook - logs completion
     */
    async postProcess(client) {
        try {
            this.logger.info('LabPracticeMappingPipeline: Processing completed successfully');
        } catch (error) {
            this.logger.error('LabPracticeMappingPipeline: Error during post-processing', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = LabPracticeMappingPipeline;
