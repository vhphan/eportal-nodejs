const {createLogger, format, transports} = require('winston');


let myFormat = format.combine(
    format.timestamp({format: 'MMM-DD-YYYY HH:mm:ss'}),
    format.align(),
    format.printf(info => `${info.level}: ${[info.timestamp]}: ${info.message}`),
);
const logger = createLogger({
    level: 'info',
    format: myFormat,
    transports: [
        //
        // - Write all logs with importance level of `error` or less to `error.log`
        // - Write all logs with importance level of `info` or less to `combined.log`
        //
        new transports.File({filename: 'error.log', level: 'error'}),
        new transports.File({filename: 'combined.log'}),
    ],
});

//
// If we're not in production then log to the `console` with the format:
// `${info.level}: ${info.message} JSON.stringify({ ...rest }) `
//
if (process.env.NODE_ENV !== 'production') {
    logger.add(new transports.Console({
        format: myFormat,
    }));
}

function logRequest(req, res, next) {
    logger.info(req.url)
    next()
}

function logError(err, req, res, next) {
    logger.error(err)
    next()
}

module.exports = {
    logger,
    logRequest,
    logError,
}