/**
 * Dental Practices Query Configuration
 * =====================================
 * SOQL query and field mapping for extracting dental practices from Salesforce.
 *
 * Target CSV columns (matching dental-practices pipeline expectations):
 * - practiceid, dentalgroupid, dentalgroupname, address, address2, city, state, zip
 * - phone, clinicalemail, billingemail, incisiveemail, feeschedule, status
 *
 * WHERE clause: Corporate_ID__c = null AND Parent.Corporate_ID__c != null
 * (practices that belong to a parent group)
 */

module.exports = {
    /**
     * Extractor name
     */
    name: 'dental-practices',

    /**
     * SOQL query to fetch dental practices from Salesforce
     * Practices are Accounts where Corporate_ID__c is null but have a Parent with Corporate_ID__c
     */
    soql: `
        SELECT
            Id,
            Practice_ID__c,
            Parent.Corporate_ID__c,
            Parent.Name,
            ShippingStreet,
            ShippingCity,
            ShippingState,
            ShippingPostalCode,
            Phone,
            Clinical_Email__c,
            Billing_Email__c,
            Incisive_Email__c,
            Fee_Schedule__c,
            Status__c
        FROM Account
        WHERE Corporate_ID__c = null
        AND Parent.Corporate_ID__c != null
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
            practiceid: record.Practice_ID__c || '',
            dentalgroupid: record.Parent?.Corporate_ID__c || '',
            dentalgroupname: record.Parent?.Name || '',
            address: record.ShippingStreet || '',
            address2: '',
            city: record.ShippingCity || '',
            state: record.ShippingState || '',
            zip: record.ShippingPostalCode || '',
            phone: record.Phone || '',
            clinicalemail: record.Clinical_Email__c || '',
            billingemail: record.Billing_Email__c || '',
            incisiveemail: record.Incisive_Email__c || '',
            feeschedule: record.Fee_Schedule__c || '',
            status: record.Status__c || ''
        };
    },

    /**
     * S3 destination path environment variable
     */
    s3EnvKey: 'DENTAL_PRACTICES_SOURCEPATH',

    /**
     * CSV filename prefix
     */
    filePrefix: 'dental-practices'
};
