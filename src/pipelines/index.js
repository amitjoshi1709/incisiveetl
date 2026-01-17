/**
 * Pipeline Registry
 * =================
 * Auto-discovers and exports all ETL pipelines from subdirectories.
 *
 * How it works:
 * 1. Scans this directory for subdirectories (excluding files and _template)
 * 2. Requires each subdirectory's index.js
 * 3. Registers pipeline by its 'name' property
 *
 * To add a new pipeline:
 * 1. Copy _template folder to new folder name (e.g., 'my-pipeline')
 * 2. Implement the pipeline class
 * 3. It will be auto-registered on next startup
 *
 * Usage:
 * const pipelines = require('./pipelines');
 * const dentalGroupsPipeline = pipelines['dental-groups'];
 */

const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

// Pipeline registry object
const pipelines = {};

// Get current directory
const pipelineDir = __dirname;

// Scan for pipeline directories
try {
    const entries = fs.readdirSync(pipelineDir, { withFileTypes: true });

    entries
        .filter(entry => {
            // Only process directories
            if (!entry.isDirectory()) return false;
            // Skip template and hidden folders
            if (entry.name.startsWith('_') || entry.name.startsWith('.')) return false;
            return true;
        })
        .forEach(entry => {
            try {
                const pipelinePath = path.join(pipelineDir, entry.name);
                const PipelineClass = require(pipelinePath);

                // Validate pipeline has required properties
                if (!PipelineClass.pipelineName) {
                    logger.warn(`Pipeline ${entry.name} missing static pipelineName, skipping`);
                    return;
                }

                // Register pipeline class (not instance)
                pipelines[PipelineClass.pipelineName] = PipelineClass;

                logger.info(`Pipeline registered: ${PipelineClass.pipelineName}`, {
                    folder: entry.name
                });
            } catch (error) {
                logger.error(`Failed to load pipeline: ${entry.name}`, {
                    error: error.message
                });
            }
        });

    logger.info(`Pipeline registry initialized`, {
        pipelineCount: Object.keys(pipelines).length,
        pipelines: Object.keys(pipelines)
    });

} catch (error) {
    logger.error('Failed to initialize pipeline registry', {
        error: error.message
    });
}

/**
 * Get all registered pipeline names
 * @returns {string[]} Array of pipeline names
 */
pipelines.getPipelineNames = () => Object.keys(pipelines).filter(k => typeof pipelines[k] !== 'function' || k === 'getPipelineNames' ? false : true);

/**
 * Check if a pipeline exists
 * @param {string} name - Pipeline name
 * @returns {boolean}
 */
pipelines.hasPipeline = (name) => !!pipelines[name] && typeof pipelines[name] !== 'function';

module.exports = pipelines;
