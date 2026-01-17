/**
 * Core Module Exports
 * ===================
 * Central export point for all core classes used by pipelines.
 */

const BasePipeline = require('./BasePipeline');
const S3Handler = require('./S3Handler');
const DatabaseConnection = require('./DatabaseConnection');

module.exports = {
    BasePipeline,
    S3Handler,
    DatabaseConnection
};
