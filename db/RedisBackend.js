const redis = require("redis");
const {logger} = require("../middleware/logger");
// const client = redis.createClient();
// await client.redisConnect();
// await client.ping();
const DEFAULT_EXPIRATION = 3600;
const client = redis.createClient();

client.on('error', (err) => console.log('Redis Client Error', err));
client.on('end', (err) => {
    console.log('Redis Client Diconnected', err);
    client.connect();
});

const redisConnect = async () => {
    if (!client.isOpen) {
        await client.connect();
    }
};

function checkCache(callback) {
    return async (req, res, next) => {
        const key = req.originalUrl.replace('/', '_');
        await redisConnect();

        try {
            const data = await client.get(key);
            if (data !== null) {
                logger.info('getting data from cache!')
                return res.status(200).send(data);
            }
            const freshData = await callback();
            await client.set(key, JSON.stringify(freshData), {EX: DEFAULT_EXPIRATION});
            return res.status(200).json(freshData);
        } catch (e) {
            next(e);
        }
    }
}

const saveToCacheGetRequest = async (req, freshData) => {
    const key = req.originalUrl.replace('/', '_');
    await redisConnect();
    await client.set(key, JSON.stringify(freshData), {EX: DEFAULT_EXPIRATION});
};

const saveToCacheKeyValue = async (key, value) => {
    await redisConnect();
    await client.set(key, JSON.stringify(value), {EX: DEFAULT_EXPIRATION});
};

const getCacheKeyValue = async (key) => {
    await redisConnect();
    const text = await client.get(key);
    if (text !== null) {
        return JSON.parse(text);
    }
    return false;
};

const checkCacheMiddleWare = async (req, res, next) => {
    const key = req.originalUrl.replace('/', '_');
    console.log(`is client open ${client.isOpen}`)
    await redisConnect();
    try {
        const data = await client.get(key);
        if (data !== null) {
            logger.info('getting data from cache!')
            return res.status(200).json({cache:true, ...JSON.parse(data)});
        }
        logger.info(`not found in cache, key=${key}`);
        next()
    } catch (e) {
        next(e);
    }
}

module.exports = {checkCache, checkCacheMiddleWare, saveToCache: saveToCacheGetRequest, saveToCacheKeyValue, getCacheKeyValue}