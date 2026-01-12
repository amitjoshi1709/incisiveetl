/**
 * Normalize CSV headers to lowercase and remove spaces/special chars
 * @param {Object} row - Raw CSV row object
 * @returns {Object} Normalized row object
 */
function normalizeCSVRow(row) {
    const normalized = {};

    for (const [key, value] of Object.entries(row)) {
        // Convert to lowercase, trim spaces, remove special characters
        const normalizedKey = key
            .toLowerCase()
            .trim()
            .replace(/[^a-z0-9]/g, ''); // Remove non-alphanumeric

        normalized[normalizedKey] = value;
    }

    return normalized;
}

/**
 * Map normalized CSV row to database schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToSchema(row) {
    return {
        submissiondate: row.submissiondate || null,
        shippingdate: row.shippingdate || null,
        casedate: row.casedate || null,
        caseid: row.caseId || null,
        productid: row.productid || null,
        productdescription: row.productdescription || null,
        quantity: row.quantity ? parseInt(row.quantity) : null,
        productprice: row.productprice || null,
        patientname: row.patientname || null,
        customerid: row.customerid || null,
        customername: row.customername || null,
        address: row.address || null,
        phonenumber: row.phonenumber || null,
        casestatus: row.casestatus || null,
        holdreason: row.holdreason || null,
        estimatecompletedate: row.estimatecompletedate || null,
        requestedreturndate: row.requestedreturndate || null,
        trackingnumber: row.trackingnumber || null,
        estimatedshipdate: row.estimatedshipdate || null,
        holddate: row.holddate || null,
        deliverystatus: row.deliverystatus || null,
        notes: row.notes || null,
        onhold: row.onhold || null,
        shade: row.shade || null,
        mold: row.mold || null,
        doctorpreferences: row.doctorpreferences || null,
        productpreferences: row.productpreferences || null,
        comments: row.comments || null,
        casetotal: row.casetotal || null
    };
}

/**
 * Debug helper to log CSV headers
 * @param {Object} row - First CSV row
 */
function logCSVHeaders(row) {
    console.log('\n=== CSV Headers Found ===');
    Object.keys(row).forEach((key, index) => {
        console.log(`${index + 1}. "${key}" (length: ${key.length})`);
    });
    console.log('========================\n');
}

function objectToCSV(data) {
    if (!data.length) return '';

    const headers = Object.keys(data[0]).join(',');

    const rows = data.map(row =>
        Object.values(row)
            .map(v => `"${String(v ?? '')
                .replace(/\r?\n/g, ' ')
                .replace(/"/g, '""')}"`)
            .join(',')
    );

    return [headers, ...rows].join('\n');
}

module.exports = {
    normalizeCSVRow,
    mapCSVToSchema,
    logCSVHeaders,
    objectToCSV
};