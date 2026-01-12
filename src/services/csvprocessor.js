const csv = require('csv-parser');
const { Readable } = require('stream');
const logger = require('../utils/logger');
const config = require('../config');
const { normalizeCSVRow, mapCSVToSchema } = require('../utils/csv-helpers');

class CSVprocessor {
  constructor(dbService) {
    this.dbService = dbService;
    this.batchSize = config.processing.batchSize;
  }

  /**
   * Parse CSV stream into rows
   */
  async parseCSV(stream) {
    return new Promise((resolve, reject) => {
      const rows = [];
      const readable = Readable.from(stream);
      readable
        .pipe(csv())
        .on('data', (row) => rows.push(row))
        .on('end', () => resolve(rows))
        .on('error', reject);
    });
  }

  /**
   * Process rows in batches
   * MODIFIED: Now tracks missing field details
   */
  async processRows(rows, fileName) {
    const client = await this.dbService.pool.connect();
    let successCount = 0;
    let errorCount = 0;
    const missingFieldErrors = []; // Track missing field details
    const csvRemark = []; // Issue marked in error row

    try {
      await client.query('BEGIN');
      await this.dbService.truncateStage(client);

      // Process in batches
      for (let i = 0; i < rows.length; i += this.batchSize) {
        const batch = rows.slice(i, i + this.batchSize);
        for (const row of batch) {
          const rowNumber = i + batch.indexOf(row) + 1;

          const normalizedRow = normalizeCSVRow(row);
          const mappedRow = mapCSVToSchema(normalizedRow);

          try {
            const requiredFields = [
              'submissiondate',
              'casedate',
              'caseid',
              'productid',
              'quantity',
              'customerid'
            ];

            const missingFields = requiredFields.filter(
              key => mappedRow[key] === null || mappedRow[key] === undefined || mappedRow[key] === ""
            )

            if (missingFields.length) {
              errorCount++;
              const errorDetail = {
                ...row,
                reason: `Missing required fields: ${missingFields.join(', ')}`,
                missingFields // array
              };

              missingFieldErrors.push(errorDetail);
              csvRemark.push(errorDetail)

              logger.error('Skipping row with missing key', {
                rowNumber,
                reason: errorDetail.reason || 'MISSING',
                caseid: mappedRow.caseid || 'MISSING',
                productid: mappedRow.productid,
                fileName
              });
            } else if (missingFields.length = 0) {
              const result = await this.dbService.insertStageRow(client, mappedRow, fileName);
              csvRemark.push(mappedRow)
              successCount++;
              logger.info('Row inserted successfully', {
                fileName,
                rowNumber,
                caseid: result.caseid,
                productid: result.productid
              });
            }
          } catch (error) {
            errorCount++;
            const errorDetail = {
              ...row,
              caseid: row.caseid || 'UNKNOWN',
              productid: row.productid || 'UNKNOWN',
              reason: error.message,
              missingField: 'ERROR'
            };
            missingFieldErrors.push(errorDetail);
            csvRemark.push(errorDetail)

            logger.error('Error inserting row', {
              rowNumber,
              error: error.message,
              fileName
            });
          }
        }

        logger.info('Batch processed', {
          batchStart: i + 1,
          batchEnd: Math.min(i + this.batchSize, rows.length),
          total: rows.length,
          successCount,
          errorCount
        });
      }

      await client.query('COMMIT');
      logger.info('Transaction committed', {
        fileName,
        totalRows: rows.length,
        successCount,
        errorCount,
        validRows: successCount,
        invalidRows: errorCount
      });

    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Transaction rolled back', { error: error.message, fileName });
      throw error;
    } finally {
      client.release();
    }

    return { successCount, errorCount, missingFieldErrors, csvRemark }; // NEW: Return error details
  }
}

module.exports = CSVprocessor;