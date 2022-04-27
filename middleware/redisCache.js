const apiCache = require('apicache');
const redis = require("redis");
let cacheWithRedis = apiCache.options({redisClient: redis.createClient(), debug: true}).middleware;

const onlyStatus200 = (req, res) => res.statusCode === 200;

const cache = apiCache.middleware
const cacheLongTerm = cacheWithRedis('10 days', onlyStatus200);
const cache15m = cacheWithRedis('15 minutes', onlyStatus200);
const cache30m = cacheWithRedis('30 minutes', onlyStatus200);
const cache12h = cacheWithRedis('12 hours', onlyStatus200);

module.exports = {
    cache,
    cacheLongTerm,
    cache15m,
    cache30m,
    cache12h
};