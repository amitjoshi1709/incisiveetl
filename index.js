/**
 * Orders ETL Service
 * ==================
 * Main entry point for the ETL service.
 *
 * Usage:
 *   node index.js                           # Process all pipelines
 *   node index.js all                       # Process all pipelines
 *   node index.js <pipeline-name>           # Process specific pipeline
 *   node index.js list                      # List available pipelines
 *   node index.js extract <extractor-name>  # Extract data from external source
 *   node index.js extract list              # List available extractors
 *
 * Available pipelines:
 *   - orders
 *   - product-catalog
 *   - dental-practices
 *   - lab-product-mapping
 *   - lab-practice-mapping
 *   - dental-groups
 *
 * Available extractors:
 *   - dental-groups (Salesforce -> S3)
 *   - dental-practices (Salesforce -> S3)
 *   - magictouch-orders (MagicTouch -> S3)
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
const { SalesforceExtractor, MagicTouchExtractor } = require('./src/extractors');
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

// ==================== Helper Functions ====================

/**
 * Map of pipelines to their corresponding extractors
 * When a pipeline runs, its extractor (if any) runs first
 */
const PIPELINE_EXTRACTORS = {
    'dental-groups': { type: 'salesforce', name: 'dental-groups' },
    'dental-practices': { type: 'salesforce', name: 'dental-practices' },
    'orders': { type: 'magictouch', name: 'orders' }
};

/**
 * Run extractor for a pipeline if one exists
 * @param {string} pipelineName - Name of the pipeline
 * @returns {Promise<Object|null>} Extraction result or null if no extractor
 */
async function runExtractorForPipeline(pipelineName) {
    const extractorConfig = PIPELINE_EXTRACTORS[pipelineName];
    if (!extractorConfig) {
        return null;
    }

    const { type, name } = extractorConfig;

    // Salesforce extractor
    if (type === 'salesforce') {
        if (!SalesforceExtractor.hasExtractor(name)) {
            return null;
        }
        logger.info(`Running Salesforce extractor before pipeline: ${name}`);
        const extractor = new SalesforceExtractor(config.salesforce, s3Handler);
        const result = await extractor.extract(name);
        logger.info(`Extractor ${name} completed`, {
            recordCount: result.recordCount,
            s3Key: result.s3Key
        });
        return result;
    }

    // MagicTouch extractor
    if (type === 'magictouch') {
        if (!MagicTouchExtractor.hasExtractor(name)) {
            return null;
        }
        logger.info(`Running MagicTouch extractor before pipeline: ${name}`);
        const extractor = new MagicTouchExtractor(config.magictouch, s3Handler);
        const result = await extractor.extract();
        logger.info(`MagicTouch extractor completed`, {
            caseCount: result.caseCount,
            recordCount: result.recordCount,
            s3Key: result.s3Key
        });
        return result;
    }

    return null;
}

// ==================== CLI Execution ====================

if (require.main === module) {
    const command = process.argv[2];
    const subCommand = process.argv[3];

    (async () => {
        try {
            // ==================== EXTRACT COMMAND ====================
            if (command === 'extract') {
                // Get all available extractors
                const sfExtractors = SalesforceExtractor.getAvailableExtractors();
                const mtExtractors = MagicTouchExtractor.getAvailableExtractors().map(e => `magictouch-${e}`);
                const allExtractors = [...sfExtractors, ...mtExtractors];

                // List available extractors
                if (subCommand === 'list' || !subCommand) {
                    console.log('\nAvailable extractors:');
                    console.log('  Salesforce:');
                    sfExtractors.forEach(name => console.log(`    - ${name}`));
                    console.log('  MagicTouch:');
                    mtExtractors.forEach(name => console.log(`    - ${name}`));
                    console.log('\nUsage: node index.js extract <extractor-name>');
                    console.log('       node index.js extract list\n');
                    await shutdown();
                    return;
                }

                // Run Salesforce extractor
                if (SalesforceExtractor.hasExtractor(subCommand)) {
                    logger.info(`Running Salesforce extractor: ${subCommand}`);
                    const extractor = new SalesforceExtractor(config.salesforce, s3Handler);
                    const result = await extractor.extract(subCommand);
                    logger.info(`Extractor ${subCommand} completed`, {
                        recordCount: result.recordCount,
                        s3Key: result.s3Key
                    });
                    console.log(`\nExtraction complete!`);
                    console.log(`  Records: ${result.recordCount}`);
                    console.log(`  File: s3://${result.bucket}/${result.s3Key}\n`);
                    await shutdown();
                    return;
                }

                // Run MagicTouch extractor
                if (subCommand.startsWith('magictouch-')) {
                    const mtExtractorName = subCommand.replace('magictouch-', '');
                    if (MagicTouchExtractor.hasExtractor(mtExtractorName)) {
                        logger.info(`Running MagicTouch extractor: ${mtExtractorName}`);
                        const extractor = new MagicTouchExtractor(config.magictouch, s3Handler);
                        const result = await extractor.extract();
                        logger.info(`Extractor ${subCommand} completed`, {
                            caseCount: result.caseCount,
                            recordCount: result.recordCount,
                            s3Key: result.s3Key
                        });
                        console.log(`\nExtraction complete!`);
                        console.log(`  Cases: ${result.caseCount}`);
                        console.log(`  Rows: ${result.recordCount}`);
                        console.log(`  File: s3://${result.bucket}/${result.s3Key}\n`);
                        await shutdown();
                        return;
                    }
                }

                // Unknown extractor
                console.error(`\nUnknown extractor: ${subCommand}`);
                console.log('\nAvailable extractors:');
                allExtractors.forEach(name => console.log(`  - ${name}`));
                console.log('\nUsage: node index.js extract <extractor-name>\n');
                process.exit(1);
            }

            // ==================== LIST COMMAND ====================
            // Show available pipelines
            if (command === 'list') {
                const available = orchestrator.getAvailablePipelines();
                console.log('\nAvailable pipelines:');
                available.forEach(name => console.log(`  - ${name}`));
                console.log('\nUsage: node index.js <pipeline-name>');
                console.log('       node index.js all');
                console.log('       node index.js extract <extractor-name>\n');
                await shutdown();
                return;
            }

            // Process all pipelines
            if (!command || command === 'all') {
                logger.info('Processing all pipelines...');

                // Run all extractors first
                for (const pipelineName of Object.keys(PIPELINE_EXTRACTORS)) {
                    try {
                        await runExtractorForPipeline(pipelineName);
                    } catch (error) {
                        logger.error(`Extractor failed for ${pipelineName}, continuing with ETL`, {
                            error: error.message
                        });
                    }
                }

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
                // Run extractor first if one exists for this pipeline
                try {
                    await runExtractorForPipeline(command);
                } catch (error) {
                    logger.error(`Extractor failed for ${command}, continuing with ETL`, {
                        error: error.message
                    });
                }

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
            console.log('       node index.js list');
            console.log('       node index.js extract <extractor-name>\n');
            process.exit(1);

        } catch (error) {
            logger.error('Fatal error', { error: error.message, stack: error.stack });
            process.exit(1);
        }
    })();
}
