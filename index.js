/**
 * Orders ETL Service
 * ==================
 * Main entry point for the ETL service.
 *
 * Usage:
 *   node index.js                    # Process all pipelines
 *   node index.js all                # Process all pipelines
 *   node index.js <pipeline-name>    # Process specific pipeline
 *   node index.js list               # List available pipelines
 *
 * Available pipelines:
 *   - orders
 *   - product-catalog
 *   - dental-practices
 *   - lab-product-mapping
 *   - lab-practice-mapping
 *   - dental-groups
 *
 * To add a new pipeline:
 *   1. Copy src/pipelines/_template to src/pipelines/<your-pipeline>
 *   2. Implement the pipeline class
 *   3. Add environment variable: <YOUR_PIPELINE>_SOURCEPATH
 *   4. Restart - it auto-registers!
 */

const config = require('./src/config');
const { S3Handler, DatabaseConnection } = require('./src/core');
const Orchestrator = require('./src/etl/Orchestrator');
const pipelines = require('./src/pipelines');
const logger = require('./src/utils/logger');

// ==================== Initialize Services ====================

const s3Handler = new S3Handler(config.aws);
const dbConnection = new DatabaseConnection(config.db);
const orchestrator = new Orchestrator(s3Handler, dbConnection, pipelines, config);

// ==================== Graceful Shutdown ====================

async function shutdown() {
    logger.info('Shutting down gracefully...');
    await dbConnection.close();
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ==================== Module Exports ====================

/**
 * Export for use as a module
 * Allows programmatic access to ETL functionality
 */
module.exports = {
    // Process specific pipeline
    processPipeline: (name) => orchestrator.processPipeline(name),

    // Process all pipelines
    processAllPipelines: () => orchestrator.processAllPipelines(),

    // Get list of available pipelines
    getAvailablePipelines: () => orchestrator.getAvailablePipelines(),

    // Core services (for advanced usage)
    s3Handler,
    dbConnection,
    orchestrator,
    pipelines,
    config
};

// ==================== CLI Execution ====================

if (require.main === module) {
    const command = process.argv[2];

    (async () => {
        try {
            // Show available pipelines
            if (command === 'list') {
                const available = orchestrator.getAvailablePipelines();
                console.log('\nAvailable pipelines:');
                available.forEach(name => console.log(`  - ${name}`));
                console.log('\nUsage: node index.js <pipeline-name>');
                console.log('       node index.js all\n');
                await shutdown();
                return;
            }

            // Process all pipelines
            if (!command || command === 'all') {
                logger.info('Processing all pipelines...');
                const results = await orchestrator.processAllPipelines();
                logger.info('All pipelines completed', {
                    totalProcessed: results.processed,
                    successful: results.successful,
                    failed: results.failed
                });
                await shutdown();
                return;
            }

            // Process specific pipeline
            const available = orchestrator.getAvailablePipelines();
            if (available.includes(command)) {
                logger.info(`Processing pipeline: ${command}`);
                const result = await orchestrator.processPipeline(command);
                logger.info(`Pipeline ${command} completed`, {
                    processed: result.processed,
                    successful: result.successful,
                    failed: result.failed
                });
                await shutdown();
                return;
            }

            // Unknown command
            console.error(`\nUnknown command: ${command}`);
            console.log('\nAvailable pipelines:');
            available.forEach(name => console.log(`  - ${name}`));
            console.log('\nUsage: node index.js <pipeline-name>');
            console.log('       node index.js all');
            console.log('       node index.js list\n');
            process.exit(1);

        } catch (error) {
            logger.error('Fatal error', { error: error.message, stack: error.stack });
            process.exit(1);
        }
    })();
}
