/**
 * Application Configuration
 * =========================
 * Central configuration module for the ETL service.
 *
 * Configuration is loaded from environment variables.
 * See .env.example for required variables.
 *
 * Note: Pipeline-specific S3 paths are handled by each pipeline
 * using their own environment variable (e.g., SOURCEPATH, LAB_PRODUCT_SOURCEPATH)
 */

require('dotenv').config();

/**
 * Get optional environment variable with default value
 * @param {string} name - Environment variable name
 * @param {string} defaultValue - Default if not set
 * @returns {string}
 */
function getOptionalEnv(name, defaultValue) {
    return process.env[name] || defaultValue;
}

/**
 * Parse boolean from environment variable
 * @param {string} name - Environment variable name
 * @param {boolean} defaultValue - Default if not set
 * @returns {boolean}
 */
function getBooleanEnv(name, defaultValue) {
    const value = process.env[name];
    if (value === undefined) return defaultValue;
    return value === 'true' || value === '1';
}

module.exports = {
    /**
     * AWS / S3 Configuration
     * Pipeline-specific paths are loaded from env vars by each pipeline
     */
    aws: {
        region: getOptionalEnv('AWS_REGION', 'us-east-1'),
        bucket: getOptionalEnv('S3_BUCKET', 'dev-incisive-data-csv'),
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
        }
    },

    /**
     * Salesforce Configuration
     * Used for extracting data from Salesforce via JWT authentication
     */
    salesforce: {
        loginUrl: getOptionalEnv('SF_LOGIN_URL', 'https://login.salesforce.com'),
        clientId: process.env.SF_CLIENT_ID,
        username: process.env.SF_USERNAME,
        privateKeyPath: process.env.SF_PRIVATE_KEY_PATH,
        privateKey: process.env.SF_PRIVATE_KEY, // Alternative: key content directly (for Docker)
        apiVersion: getOptionalEnv('SF_API_VERSION', '59.0')
    },

    /**
     * Database Configuration
     */
    db: {
        host: getOptionalEnv('DB_HOST', 'localhost'),
        port: parseInt(getOptionalEnv('DB_PORT', '5432'), 10),
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: getOptionalEnv('DB_NAME', 'postgres'),
        // Set search path for ETL schema
        options: '-c search_path=etl,public',
        // Connection pool settings
        max: parseInt(getOptionalEnv('DB_POOL_MAX', '10'), 10),
        idleTimeoutMillis: parseInt(getOptionalEnv('DB_IDLE_TIMEOUT', '30000'), 10),
        connectionTimeoutMillis: parseInt(getOptionalEnv('DB_CONNECT_TIMEOUT', '2000'), 10),
        // SSL configuration
        ssl: getBooleanEnv('DB_SSL', false)
            ? { rejectUnauthorized: !getBooleanEnv('DB_SSL_REJECT_UNAUTHORIZED', true) }
            : false
    },

    /**
     * Processing Configuration
     */
    processing: {
        // Number of rows to process per batch
        batchSize: parseInt(getOptionalEnv('BATCH_SIZE', '100'), 10)
    },

    /**
     * Logging Configuration
     */
    logging: {
        level: getOptionalEnv('LOG_LEVEL', 'info'),
        // Directory for local log files
        directory: getOptionalEnv('LOG_DIR', './logs')
    }
};
