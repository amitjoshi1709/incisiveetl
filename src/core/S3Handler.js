/**
 * S3Handler.js
 * ============
 * Centralized S3 operations handler for all ETL pipelines.
 * Provides generic methods that work with any pipeline by accepting path parameters.
 *
 * Features:
 * - File existence checking
 * - File retrieval (streaming)
 * - File listing with pagination
 * - Move to processed folder (with timestamp)
 * - Log file uploading
 *
 * Usage:
 * const s3Handler = new S3Handler(config);
 * const files = await s3Handler.listFiles(sourcePath, processedPath);
 * const stream = await s3Handler.getFile(fileName, sourcePath);
 */

const {
    S3Client,
    GetObjectCommand,
    CopyObjectCommand,
    DeleteObjectCommand,
    ListObjectsV2Command,
    PutObjectCommand
} = require('@aws-sdk/client-s3');
const fs = require('fs/promises');
const logger = require('../utils/logger');

class S3Handler {
    /**
     * Initialize S3 client with AWS configuration
     * @param {Object} awsConfig - AWS configuration object
     * @param {string} awsConfig.region - AWS region
     * @param {string} awsConfig.bucket - S3 bucket name
     * @param {Object} [awsConfig.credentials] - Optional AWS credentials
     */
    constructor(awsConfig) {
        const s3Config = { region: awsConfig.region };

        // Use explicit credentials if provided, otherwise use default chain
        if (awsConfig.credentials?.accessKeyId && awsConfig.credentials?.secretAccessKey) {
            s3Config.credentials = awsConfig.credentials;
            logger.info('S3Handler: Using AWS credentials from environment variables');
        } else {
            logger.info('S3Handler: Using AWS credentials from default credential chain');
        }

        this.client = new S3Client(s3Config);
        this.bucket = awsConfig.bucket;
    }

    /**
     * Ensure S3 folder exists (creates empty placeholder if not)
     * @param {string} folderPath - S3 folder path (must end with /)
     * @returns {Promise<void>}
     */
    async ensureFolderExists(folderPath) {
        try {
            // Check if folder exists
            const command = new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: folderPath,
                MaxKeys: 1
            });

            const response = await this.client.send(command);

            // If folder doesn't exist, create it
            if (!response.Contents || response.Contents.length === 0) {
                const putCommand = new PutObjectCommand({
                    Bucket: this.bucket,
                    Key: folderPath,
                    Body: Buffer.alloc(0),
                    ContentLength: 0
                });
                await this.client.send(putCommand);
                logger.info('S3Handler: Folder created', { folderPath, bucket: this.bucket });
            }
        } catch (error) {
            logger.error('S3Handler: Error ensuring folder exists', {
                folderPath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Check if a file exists in S3
     * @param {string} fileName - Name of the file
     * @param {string} sourcePath - S3 prefix/folder path
     * @returns {Promise<boolean>} True if file exists
     */
    async checkFileExists(fileName, sourcePath) {
        try {
            const command = new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: sourcePath + fileName,
                MaxKeys: 1
            });

            const response = await this.client.send(command);
            return response.Contents && response.Contents.length > 0;
        } catch (error) {
            logger.error('S3Handler: Error checking file existence', {
                fileName,
                sourcePath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Get file content from S3 as a stream
     * @param {string} fileName - Name of the file
     * @param {string} sourcePath - S3 prefix/folder path
     * @returns {Promise<ReadableStream>} File content stream
     */
    async getFile(fileName, sourcePath) {
        try {
            const key = sourcePath + fileName;
            const command = new GetObjectCommand({
                Bucket: this.bucket,
                Key: key
            });

            const response = await this.client.send(command);
            logger.info('S3Handler: File retrieved successfully', { fileName, key });
            return response.Body;
        } catch (error) {
            logger.error('S3Handler: Error getting file', {
                fileName,
                sourcePath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * List all CSV files in a source path (excludes processed folder)
     * Supports pagination for large buckets
     * @param {string} sourcePath - S3 prefix to search
     * @param {string} processedPath - Processed folder path to exclude
     * @returns {Promise<string[]>} Array of file names (without path prefix)
     */
    async listFiles(sourcePath, processedPath) {
        try {
            const allFiles = [];
            let continuationToken = undefined;

            do {
                const command = new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: sourcePath,
                    ContinuationToken: continuationToken
                });

                const response = await this.client.send(command);

                if (response.Contents && response.Contents.length > 0) {
                    const files = response.Contents
                        .filter(obj => {
                            // Exclude folder itself, processed folder, and non-CSV files
                            return obj.Key !== sourcePath &&
                                !obj.Key.startsWith(processedPath) &&
                                obj.Key.endsWith('.csv');
                        })
                        .map(obj => obj.Key.replace(sourcePath, ''));

                    allFiles.push(...files);
                }

                continuationToken = response.IsTruncated
                    ? response.NextContinuationToken
                    : undefined;

            } while (continuationToken);

            logger.info('S3Handler: Files listed', {
                sourcePath,
                fileCount: allFiles.length
            });

            return allFiles;
        } catch (error) {
            logger.error('S3Handler: Error listing files', {
                sourcePath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Move file to processed folder with timestamp prefix
     * @param {string} fileName - Name of the file to move
     * @param {string} sourcePath - Source folder path
     * @param {string} processedPath - Destination processed folder path
     * @returns {Promise<string>} New file key in processed folder
     */
    async moveToProcessed(fileName, sourcePath, processedPath) {
        try {
            const sourceKey = sourcePath + fileName;
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const destKey = processedPath + `${timestamp}_${fileName}`;

            // Step 1: Copy to processed folder
            const copyCommand = new CopyObjectCommand({
                Bucket: this.bucket,
                CopySource: `${this.bucket}/${sourceKey}`,
                Key: destKey
            });
            await this.client.send(copyCommand);
            logger.info('S3Handler: File copied to processed', { sourceKey, destKey });

            // Step 2: Delete original
            const deleteCommand = new DeleteObjectCommand({
                Bucket: this.bucket,
                Key: sourceKey
            });
            await this.client.send(deleteCommand);
            logger.info('S3Handler: Original file deleted', { sourceKey });

            return destKey;
        } catch (error) {
            logger.error('S3Handler: Error moving file to processed', {
                fileName,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Upload a local log/report file to S3
     * @param {string} localFilePath - Path to local file
     * @param {string} fileName - Base name for S3 file
     * @param {string} logsPath - S3 folder path for logs
     * @returns {Promise<string>} S3 key of uploaded file
     */
    async uploadLogFile(localFilePath, fileName, logsPath) {
        try {
            const fileContent = await fs.readFile(localFilePath);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const baseName = fileName.replace(/\.csv$/i, '');
            const s3Key = `${logsPath}${baseName}_log_${timestamp}.csv`;

            const command = new PutObjectCommand({
                Bucket: this.bucket,
                Key: s3Key,
                Body: fileContent,
                ContentType: 'text/csv'
            });

            await this.client.send(command);
            logger.info('S3Handler: Log file uploaded', {
                localPath: localFilePath,
                s3Key,
                bucket: this.bucket
            });

            return s3Key;
        } catch (error) {
            logger.error('S3Handler: Error uploading log file', {
                localFilePath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Upload processed file (valid rows only) to S3
     * @param {string} localFilePath - Path to local file
     * @param {string} fileName - Base name for S3 file
     * @param {string} processedPath - S3 folder path for processed files
     * @returns {Promise<string>} S3 key of uploaded file
     */
    async uploadProcessedFile(localFilePath, fileName, processedPath) {
        try {
            const fileContent = await fs.readFile(localFilePath);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const s3Key = `${processedPath}${timestamp}_${fileName}`;

            const command = new PutObjectCommand({
                Bucket: this.bucket,
                Key: s3Key,
                Body: fileContent,
                ContentType: 'text/csv'
            });

            await this.client.send(command);
            logger.info('S3Handler: Processed file uploaded', {
                localPath: localFilePath,
                s3Key,
                bucket: this.bucket
            });

            return s3Key;
        } catch (error) {
            logger.error('S3Handler: Error uploading processed file', {
                localFilePath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Delete a file from S3
     * @param {string} fileName - Name of the file to delete
     * @param {string} sourcePath - S3 folder path
     * @returns {Promise<void>}
     */
    async deleteFile(fileName, sourcePath) {
        try {
            const key = sourcePath + fileName;
            const command = new DeleteObjectCommand({
                Bucket: this.bucket,
                Key: key
            });

            await this.client.send(command);
            logger.info('S3Handler: File deleted', { key, bucket: this.bucket });
        } catch (error) {
            logger.error('S3Handler: Error deleting file', {
                fileName,
                sourcePath,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Build S3 paths for a pipeline from environment variable
     * @param {string} basePath - Base path from environment variable
     * @returns {{ sourcePath: string|null, processedPath: string|null, logsPath: string|null }}
     */
    static buildPipelinePaths(basePath) {
        if (!basePath) {
            return { sourcePath: null, processedPath: null, logsPath: null };
        }
        return {
            sourcePath: `${basePath}/`,
            processedPath: `${basePath}/processed/`,
            logsPath: `${basePath}/logs/`
        };
    }
}

module.exports = S3Handler;
