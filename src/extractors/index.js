/**
 * Extractor Registry
 * ==================
 * Central registry for data extraction modules.
 *
 * Currently supported extractors:
 * - salesforce: Extract data from Salesforce using JWT authentication
 *
 * Usage:
 * const { SalesforceExtractor } = require('./extractors');
 * const extractor = new SalesforceExtractor(config, s3Handler);
 * await extractor.extract('dental-groups');
 */

const SalesforceExtractor = require('./salesforce');

module.exports = {
    SalesforceExtractor
};
