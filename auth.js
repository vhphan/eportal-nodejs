const MySQLBackend = require("./db/MySQLBackend");
const {getCookies} = require("./db/utils");
const {saveToCache, saveToCacheKeyValue, getCacheKeyValue} = require("./db/RedisBackend");
const {logger} = require("./middleware/logger");

const checkApi = async (apiKey, operator) => {
    let mySQLBackend;
    let sqlQuery;
    mySQLBackend = new MySQLBackend(operator);
    let numRows;

    numRows = await getCacheKeyValue(`${apiKey}-${operator}`)
    if (numRows) {
        logger.info('got the apiKey response from redis')
        return !!numRows;
    }

    switch (operator) {
        case 'dnb':
            sqlQuery = "SELECT * FROM eproject_dnb.tbluser WHERE API_token = ? ORDER BY UserID DESC LIMIT 1";
            break;
        case 'celcom':
            sqlQuery = "SELECT * FROM eproject_cm.tbluser WHERE API_token = ? ORDER BY UserID DESC LIMIT 1";
            break;
    }
    numRows = await mySQLBackend.numRows(sqlQuery, [apiKey]);
    saveToCacheKeyValue(`${apiKey}-${operator}`, numRows).then(()=>logger.info('saved apiKey response to redis'));
    return !!numRows;
};

const auth = (operator) => {
    return async (req, res, next) => {
        const apiKey = req.headers['API'] || req.headers['api'] || getCookies(req)['API'];
        if (!apiKey) {
            const err = new Error('No api key! You are not authenticated! Please login!')
            return res.status(401).json({
                error: err.message
            });
        }
        if (await checkApi(apiKey, operator)) {
            return next();
        } else {
            const err = new Error('You are not authenticated! Please login!')
            // err.status = 401
            // return next(err)
            return res.status(401).json({
                error: err.message
            });
        }
    };
}

module.exports = auth;