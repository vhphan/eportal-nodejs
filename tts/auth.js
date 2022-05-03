const ttsQueries = require('../db/pgjs/ttsQueries');
const asyncHandler = require("../middleware/async");
const {getCacheKeyValue, saveToCacheKeyValue} = require("../db/RedisBackend");
const {logger} = require("../middleware/logger");
const {getCookies} = require("../db/utils");

const checkUserPassword = asyncHandler(async (req, res, next) => {
    const {username, password} = req.body;
    const user = await ttsQueries.getUser(username, password);
    if (!user) {
        return res.status(400).json({
            success: false,
            message: "Invalid credentials"
        });
    }
    req.user = user;
    next();
});


const checkApiKey = async (apiKey) => {

    let user = await getCacheKeyValue(apiKey);
    if (user) {
        return true;
    }

    user = await ttsQueries.getUserByApiKey(apiKey);
    if (user.length === 0) {
        return false;
    }

    saveToCacheKeyValue(apiKey, JSON.stringify(user)).then(() => {
        logger.info(`API ${apiKey} key saved to cache`);
    });

    return true;

}

const auth = async (req, res, next) => {

    const apiKey = req.headers['API'] || req.headers['api'] || getCookies(req)['API'] || req.query['api'] || req.query['API'];
    if (!apiKey) {
        const err = new Error('No api key! You are not authenticated! Please login!')
        return res.status(401).json({
            success: false,
            message: err.message
        });
    }

    if (await checkApiKey(apiKey)) {
        return next();
    }
    const err = new Error('You are not authenticated! Please login!')
    return res.status(401).json({
        error: err.message
    });

};


module.exports = {
    checkUserPassword,
    auth
};