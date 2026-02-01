/**
 * Dental Groups Query Configuration
 * ==================================
 * SOQL query and field mapping for extracting dental groups from Salesforce.
 *
 * Target CSV columns (matching dental-groups pipeline expectations):
 * - dentalgroupid, name, address, address2, city, state, zip
 * - accounttype, centralizedbilling, saleschannel, salesrep
 */

module.exports = {
    /**
     * Extractor name
     */
    name: 'dental-groups',

    /**
     * SOQL query to fetch dental groups from Salesforce
     * Adjust field names to match your Salesforce schema
     */
    soql: `
        SELECT
            Corporate_ID__c,
            Name,
            ShippingStreet,
            ShippingCity,
            ShippingState,
            ShippingPostalCode,
            Practice_Type__c,
            Centralized_Billing__c,
            Sales_Channel__c,
            Owner.Name
        FROM Account
        WHERE Corporate_ID__c != null
        ORDER BY Name
    `.trim(),

    /**
     * Map Salesforce record to CSV row format
     * Keys match the CSV column headers expected by the ETL pipeline
     *
     * @param {Object} record - Salesforce record
     * @returns {Object} CSV row object
     */
    mapRecord(record) {
        return {
            dentalgroupid: record.Corporate_ID__c || '',
            name: record.Name || '',
            address: record.ShippingStreet || '',
            address2: '', // No separate address2 field in standard SF
            city: record.ShippingCity || '',
            state: record.ShippingState || '',
            zip: record.ShippingPostalCode || '',
            accounttype: record.Account_Type__c || '',
            centralizedbilling: record.Centralized_Billing__c != null
                ? String(record.Centralized_Billing__c).toLowerCase()
                : '',
            saleschannel: record.Sales_Channel__c || '',
            salesrep: record.Owner?.Name || ''
        };
    },

    /**
     * S3 destination path environment variable
     */
    s3EnvKey: 'DENTAL_GROUPS_SOURCEPATH',

    /**
     * CSV filename prefix
     */
    filePrefix: 'dental-groups'
};
