const winston = require('winston');
const path = require('path');
const fs = require('fs');

const logDir = path.join(__dirname, '..', '..', 'logs');

if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
}

// Default logger for general/combined logs
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    transports: [
        new winston.transports.File({
            filename: path.join(logDir, 'error.log'),
            level: 'error'
        }),
        new winston.transports.File({
            filename: path.join(logDir, 'combined.log')
        })
    ]
});

// Cache for pipeline-specific loggers
const pipelineLoggers = {};

/**
 * Create or get a pipeline-specific logger
 * Creates separate log files for each pipeline
 * @param {string} pipelineName - Name of the pipeline
 * @returns {winston.Logger} Pipeline-specific logger
 */
function getPipelineLogger(pipelineName) {
    if (!pipelineName) {
        return logger;
    }

    // Return cached logger if exists
    if (pipelineLoggers[pipelineName]) {
        return pipelineLoggers[pipelineName];
    }

    // Create pipeline-specific log directory
    const pipelineLogDir = path.join(logDir, pipelineName);
    if (!fs.existsSync(pipelineLogDir)) {
        fs.mkdirSync(pipelineLogDir, { recursive: true });
    }

    // Create new logger for this pipeline
    const pipelineLogger = winston.createLogger({
        level: process.env.LOG_LEVEL || 'info',
        format: winston.format.combine(
            winston.format.timestamp(),
            winston.format.errors({ stack: true }),
            winston.format.json()
        ),
        transports: [
            // Pipeline-specific error log
            new winston.transports.File({
                filename: path.join(pipelineLogDir, 'error.log'),
                level: 'error'
            }),
            // Pipeline-specific combined log
            new winston.transports.File({
                filename: path.join(pipelineLogDir, 'combined.log')
            }),
            // Also write to main combined log
            new winston.transports.File({
                filename: path.join(logDir, 'combined.log')
            }),
            // Also write errors to main error log
            new winston.transports.File({
                filename: path.join(logDir, 'error.log'),
                level: 'error'
            })
        ]
    });

    // Cache the logger
    pipelineLoggers[pipelineName] = pipelineLogger;

    return pipelineLogger;
}

/**
 * Clear all pipeline log files (useful for fresh runs)
 * @param {string} pipelineName - Optional pipeline name to clear specific logs
 */
function clearLogs(pipelineName) {
    try {
        if (pipelineName) {
            const pipelineLogDir = path.join(logDir, pipelineName);
            if (fs.existsSync(pipelineLogDir)) {
                fs.writeFileSync(path.join(pipelineLogDir, 'combined.log'), '');
                fs.writeFileSync(path.join(pipelineLogDir, 'error.log'), '');
            }
        } else {
            fs.writeFileSync(path.join(logDir, 'combined.log'), '');
            fs.writeFileSync(path.join(logDir, 'error.log'), '');
        }
    } catch (error) {
        console.error('Error clearing logs:', error.message);
    }
}

// Export default logger and helper functions
module.exports = logger;
module.exports.getPipelineLogger = getPipelineLogger;
module.exports.clearLogs = clearLogs;
