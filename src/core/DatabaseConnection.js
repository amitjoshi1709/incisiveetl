/**
 * DatabaseConnection.js
 * =====================
 * Centralized PostgreSQL connection pool management.
 * Provides a singleton-like pattern for database connections used by all pipelines.
 *
 * Features:
 * - Connection pooling with configurable limits
 * - Automatic error handling and logging
 * - Graceful shutdown support
 * - Health check capability
 *
 * Usage:
 * const db = new DatabaseConnection(config.db);
 * const pool = db.getPool();
 * await db.close();
 */

const { Pool } = require('pg');
const logger = require('../utils/logger');

class DatabaseConnection {
    /**
     * Initialize database connection pool
     * @param {Object} dbConfig - Database configuration
     * @param {string} dbConfig.host - Database host
     * @param {number} dbConfig.port - Database port
     * @param {string} dbConfig.user - Database user
     * @param {string} dbConfig.password - Database password
     * @param {string} dbConfig.database - Database name
     * @param {number} [dbConfig.max] - Maximum pool connections
     * @param {number} [dbConfig.idleTimeoutMillis] - Idle connection timeout
     * @param {number} [dbConfig.connectionTimeoutMillis] - Connection timeout
     */
    constructor(dbConfig) {
        this.pool = new Pool(dbConfig);
        this.isConnected = false;

        // Handle unexpected errors on idle clients
        this.pool.on('error', (err) => {
            logger.error('DatabaseConnection: Unexpected pool error', {
                error: err.message,
                stack: err.stack
            });
        });

        // Log successful connection on first connect
        this.pool.on('connect', () => {
            if (!this.isConnected) {
                this.isConnected = true;
                logger.info('DatabaseConnection: Pool connected to database');
            }
        });

        logger.info('DatabaseConnection: Pool initialized', {
            host: dbConfig.host,
            database: dbConfig.database,
            maxConnections: dbConfig.max || 10
        });
    }

    /**
     * Get the connection pool for direct use
     * @returns {Pool} PostgreSQL connection pool
     */
    getPool() {
        return this.pool;
    }

    /**
     * Get a client from the pool (remember to release!)
     * @returns {Promise<PoolClient>} Database client
     */
    async getClient() {
        return this.pool.connect();
    }

    /**
     * Execute a query using the pool
     * @param {string} sql - SQL query
     * @param {any[]} [values] - Query parameters
     * @returns {Promise<QueryResult>} Query result
     */
    async query(sql, values = []) {
        try {
            return await this.pool.query(sql, values);
        } catch (error) {
            logger.error('DatabaseConnection: Query error', {
                error: error.message,
                sql: sql.substring(0, 100) // Log first 100 chars of query
            });
            throw error;
        }
    }

    /**
     * Check database connectivity
     * @returns {Promise<boolean>} True if connected
     */
    async healthCheck() {
        try {
            await this.pool.query('SELECT 1');
            return true;
        } catch (error) {
            logger.error('DatabaseConnection: Health check failed', {
                error: error.message
            });
            return false;
        }
    }

    /**
     * Get pool statistics
     * @returns {Object} Pool stats (total, idle, waiting)
     */
    getStats() {
        return {
            totalConnections: this.pool.totalCount,
            idleConnections: this.pool.idleCount,
            waitingClients: this.pool.waitingCount
        };
    }

    /**
     * Close the connection pool gracefully
     * Should be called during application shutdown
     */
    async close() {
        try {
            await this.pool.end();
            this.isConnected = false;
            logger.info('DatabaseConnection: Pool closed gracefully');
        } catch (error) {
            logger.error('DatabaseConnection: Error closing pool', {
                error: error.message
            });
            throw error;
        }
    }
}

module.exports = DatabaseConnection;
