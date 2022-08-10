const MySQLBackend = require("./db/MySQLBackend");
const {getCookies} = require("./db/utils");
const {saveToCacheKeyValue, getCacheKeyValue} = require("./db/RedisBackend");
const {logger} = require("./middleware/logger");

async function getUser(operator, apiKey) {
    let user = await getCacheKeyValue(`${apiKey}-${operator}-user`)
    if (user) {
        logger.info('got the user response from redis')
        return user;
    }
    const mySQLBackend = new MySQLBackend(operator);
    let sqlQuery;
    switch (operator) {
        case 'dnb':
            sqlQuery = "SELECT * FROM eproject_dnb.tbluser WHERE API_token = ? ORDER BY UserID DESC LIMIT 1";
            break;
        case 'celcom':
            sqlQuery = "SELECT * FROM eproject_cm.tbluser WHERE API_token = ? ORDER BY UserID DESC LIMIT 1";
            break;
    }
    const [rows, fields] = await mySQLBackend.query(sqlQuery, [apiKey]);
    user = rows[0];
    if (user) {
        saveToCacheKeyValue(`${apiKey}-${operator}-user`, user).then(() => logger.info(`saved user ${user.Name} response to redis operator=${operator}`));
    }
    return user;
}

const checkApi = async (apiKey, operator) => {
    console.log(process.env.NODE_ENV);
    logger.info(`checking api key ${apiKey}`)
    console.log(`i am checking api key ${apiKey}`)
    const user = await getUser(operator, apiKey);
    return !!user;
};

const checkUserPassword = async (username, password, operator) => {
    let mySQLBackend;
    let sqlQuery;
    mySQLBackend = new MySQLBackend(operator);
    let numRows;
    numRows = await getCacheKeyValue(`${username}-${password}-${operator}`)
    if (numRows) {
        logger.info('got the user/pw response from redis')
        return !!numRows;
    }
    switch (operator) {
        case 'dnb':
            sqlQuery = "SELECT * FROM eproject_dnb.tbluser WHERE Username = ? AND Password = ? ORDER BY UserID DESC LIMIT 1";
            break;
        case 'celcom':
            sqlQuery = "SELECT * FROM eproject_cm.tbluser WHERE Username = ? AND Password = ? ORDER BY UserID DESC LIMIT 1";
            break;
    }
    numRows = await mySQLBackend.numRows(sqlQuery, [username, password]);
    saveToCacheKeyValue(`${username}-${password}-${operator}`, numRows).then(() => logger.info('saved user/pw response to redis'));
    return !!numRows;
};

const getApiKeyUsingPassword = (operator) => async (req, res) => {
    const {userName, password} = req.query;

    if (!userName || !password) {
        return res.status(401).json({
            success: false,
            error: 'Username or password is missing!'
        });
    }

    const mySQLBackend = new MySQLBackend(operator);
    let sqlQuery;
    switch (operator) {
        case 'dnb':
            sqlQuery = "SELECT * FROM eproject_dnb.tbluser WHERE Username = ? AND Password = ? ORDER BY UserID DESC LIMIT 1";
            break;
        case 'celcom':
            sqlQuery = "SELECT * FROM eproject_cm.tbluser WHERE Username = ? AND Password = ? ORDER BY UserID DESC LIMIT 1";
            break;
    }
    const [rows, fields] = await mySQLBackend.query(sqlQuery, [userName, password]);
    const user = rows[0];
    if (user) {
        return res.json({
            success: true,
            apiKey: user.API_token
        });
    }
    return res.status(401).json({
        success: false,
        error: 'Login Failed!'
    });

}

async function checkBasicAuthHeader(operator, req, next, res) {
    try {
        const base64Credentials = req.headers.authorization.split(' ')[1];
        const credentials = Buffer.from(base64Credentials, 'base64').toString('ascii');
        const [username, password] = credentials.split(':');
        if (await checkUserPassword(username, password, operator)) {
            return next();
        } else {
            return res.status(401).send('Unauthorized');
        }
    } catch (e) {
        return res.status(401).send('Unauthorized');
    }
}

const basicAuth = (operator) => {
    return async (req, res, next) => {
        if (!req.headers.authorization) return next();
        return await checkBasicAuthHeader(req, operator, next, res);
    }
}


const auth = (operator) => {
    return async (req, res, next) => {

        if (req.headers.authorization) {
            return await checkBasicAuthHeader(operator, req, next, res);
        }

        const apiKey = req.headers['API'] || req.headers['api'] || getCookies(req)['API'] || req.query['api'] || req.query['API'];

        if (!apiKey) {
            const err = new Error('No api key! You are not authenticated! Please login!')
            return res.status(401).json({
                error: err.message
            });
        }

        if (await checkApi(apiKey, operator)) {
            return next();
        } else {
            const err = new Error('Login Failed!')
            // err.status = 401
            // return next(err)
            return res.status(401).json({
                success: false,
                error: err.message
            });
        }
    };
}

module.exports = {auth, basicAuth, getUser,getApiKeyUsingPassword};