/**
 * Salesforce Client
 * =================
 * Reusable Salesforce API client with JWT authentication.
 *
 * Features:
 * - JWT authentication using private key
 * - SOQL query execution
 * - Automatic pagination handling (SF returns max 2000 records)
 * - Token caching and refresh
 *
 * Usage:
 * const client = new SalesforceClient(config);
 * await client.authenticate();
 * const records = await client.query('SELECT Id, Name FROM Account');
 */

const fs = require('fs');
const axios = require('axios');
const jwt = require('jsonwebtoken');
const logger = require('../../utils/logger');

class SalesforceClient {
    /**
     * Initialize Salesforce client
     * @param {Object} config - Salesforce configuration
     * @param {string} config.loginUrl - Salesforce login URL
     * @param {string} config.clientId - Connected App client ID
     * @param {string} config.username - Salesforce username
     * @param {string} [config.privateKeyPath] - Path to private key file
     * @param {string} [config.privateKey] - Private key content directly
     * @param {string} config.apiVersion - Salesforce API version
     */
    constructor(config) {
        this.loginUrl = config.loginUrl;
        this.clientId = config.clientId;
        this.username = config.username;
        this.privateKeyPath = config.privateKeyPath;
        this.privateKeyContent = config.privateKey;
        this.apiVersion = config.apiVersion || '59.0';

        this.accessToken = null;
        this.instanceUrl = null;
        this.tokenExpiry = null;
    }

    /**
     * Get private key from file or config
     * @returns {string} Private key content
     */
    getPrivateKey() {
        if (this.privateKeyContent) {
            // Convert literal \n to actual newlines (needed when key is from env var)
            return this.privateKeyContent.replace(/\\n/g, '\n');
        }
        if (this.privateKeyPath) {
            return fs.readFileSync(this.privateKeyPath, 'utf8');
        }
        throw new Error('No private key provided. Set SF_PRIVATE_KEY or SF_PRIVATE_KEY_PATH');
    }

    /**
     * Authenticate to Salesforce using JWT Bearer flow
     * @returns {Promise<void>}
     */
    async authenticate() {
        try {
            const privateKey = this.getPrivateKey();
            const now = Math.floor(Date.now() / 1000);

            // Create JWT assertion
            const assertion = jwt.sign(
                {
                    iss: this.clientId,
                    sub: this.username,
                    aud: this.loginUrl,
                    exp: now + 180 // 3 minutes
                },
                privateKey,
                { algorithm: 'RS256' }
            );

            const tokenUrl = `${this.loginUrl}/services/oauth2/token`;

            logger.info('SalesforceClient: Authenticating to Salesforce', {
                loginUrl: this.loginUrl,
                username: this.username
            });

            const response = await axios.post(
                tokenUrl,
                new URLSearchParams({
                    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    assertion
                }).toString(),
                { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
            );

            this.accessToken = response.data.access_token;
            this.instanceUrl = response.data.instance_url;
            this.tokenExpiry = now + 3600; // Token valid for ~1 hour

            logger.info('SalesforceClient: Authentication successful', {
                instanceUrl: this.instanceUrl
            });

        } catch (error) {
            const errorMessage = error.response?.data?.error_description || error.message;
            logger.error('SalesforceClient: Authentication failed', {
                error: errorMessage,
                loginUrl: this.loginUrl
            });
            throw new Error(`Salesforce authentication failed: ${errorMessage}`);
        }
    }

    /**
     * Check if current token is valid
     * @returns {boolean}
     */
    isAuthenticated() {
        if (!this.accessToken || !this.tokenExpiry) {
            return false;
        }
        const now = Math.floor(Date.now() / 1000);
        return now < this.tokenExpiry - 60; // Refresh 1 minute before expiry
    }

    /**
     * Ensure client is authenticated
     * @returns {Promise<void>}
     */
    async ensureAuthenticated() {
        if (!this.isAuthenticated()) {
            await this.authenticate();
        }
    }

    /**
     * Execute a SOQL query with automatic pagination
     * @param {string} soql - SOQL query string
     * @returns {Promise<Object[]>} Array of records
     */
    async query(soql) {
        await this.ensureAuthenticated();

        const allRecords = [];
        let nextUrl = `${this.instanceUrl}/services/data/v${this.apiVersion}/query?q=${encodeURIComponent(soql)}`;

        logger.info('SalesforceClient: Executing SOQL query', {
            query: soql.substring(0, 100) + (soql.length > 100 ? '...' : '')
        });

        try {
            do {
                const response = await axios.get(nextUrl, {
                    headers: { Authorization: `Bearer ${this.accessToken}` }
                });

                const data = response.data;
                allRecords.push(...data.records);

                logger.info('SalesforceClient: Query batch retrieved', {
                    batchSize: data.records.length,
                    totalSoFar: allRecords.length,
                    done: data.done
                });

                // Check for more records
                if (data.done) {
                    nextUrl = null;
                } else if (data.nextRecordsUrl) {
                    nextUrl = `${this.instanceUrl}${data.nextRecordsUrl}`;
                } else {
                    nextUrl = null;
                }

            } while (nextUrl);

            logger.info('SalesforceClient: Query completed', {
                totalRecords: allRecords.length
            });

            return allRecords;

        } catch (error) {
            const errorMessage = error.response?.data?.[0]?.message || error.message;
            logger.error('SalesforceClient: Query failed', {
                error: errorMessage,
                query: soql.substring(0, 100)
            });
            throw new Error(`Salesforce query failed: ${errorMessage}`);
        }
    }

    /**
     * Execute a SOQL query and return count only
     * @param {string} objectName - Salesforce object name
     * @param {string} [whereClause] - Optional WHERE clause
     * @returns {Promise<number>} Record count
     */
    async queryCount(objectName, whereClause = '') {
        const soql = `SELECT COUNT() FROM ${objectName}${whereClause ? ' WHERE ' + whereClause : ''}`;

        await this.ensureAuthenticated();

        try {
            const response = await axios.get(
                `${this.instanceUrl}/services/data/v${this.apiVersion}/query?q=${encodeURIComponent(soql)}`,
                { headers: { Authorization: `Bearer ${this.accessToken}` } }
            );

            return response.data.totalSize;

        } catch (error) {
            const errorMessage = error.response?.data?.[0]?.message || error.message;
            logger.error('SalesforceClient: Count query failed', { error: errorMessage });
            throw new Error(`Salesforce count query failed: ${errorMessage}`);
        }
    }
}

module.exports = SalesforceClient;
