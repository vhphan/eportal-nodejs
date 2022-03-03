const express = require('express');
const auth = require("../auth");
const router = express.Router();
const pgDb = require('../db/PostgresQueries');
const pgDbStats = require('../db/pgQueriesStats');

const apiCache = require('apicache');
// const {createListener} = require("../db/utils");
// const PostgresBackend = require("../db/PostgresBackend");
// const {checkCache, checkCacheMiddleWare} = require("../db/RedisBackend");
const redis = require("redis");


let cache = apiCache.middleware
let cacheWithRedis = apiCache.options({ redisClient: redis.createClient(), debug: true }).middleware;


router.use(auth('dnb'))

function handler(req, res) {
    return res.send('Hello dnb');
}


router.get('/', handler);
router.get('/cellInfo', cache('5 minutes'), pgDb.getCellInfo);
router.get('/fullView', cache('15 minutes'), pgDb.dbFullViewData);
router.get('/changeLog', cache('15 minutes'), pgDb.getChangeLog);
router.put('/updateNominal', pgDb.updateNominal);
router.put('/updateConfigs', pgDb.updateConfigs);
router.post('/postJob', pgDb.addJob);
router.route('/tabulatorConfig')
    .get(pgDb.getTabulatorConfig)
    .post(pgDb.saveTabulatorConfig);

const onlyStatus200 = (req, res) => res.statusCode === 200;

const cache15m = cacheWithRedis('15 minutes', onlyStatus200);
const cache12h = cacheWithRedis('12 hours', onlyStatus200);

router.get('/tabulatorData', cache15m , pgDb.getTabulatorData);

// router.route('testtest')
//     .get(getHandler)
//     .put(putHandler)
//     .post(postHandler)

router.get(
    '/networkDailyNR',
    cache15m,
    pgDbStats.dailyNetworkQueryNR
);

router.get(
    '/networkDailyLTE',
    cache15m,
    pgDbStats.dailyNetworkQueryLTE
);

router.get(
    '/plmnDailyNR',
    cache15m,
    pgDbStats.dailyPlmnQueryNR
);

router.get(
    '/plmnDailyLTE',
    cache15m,
    pgDbStats.dailyPlmnQueryLTE
);

router.get(
    '/siteList',
    cache12h,
    pgDbStats.siteListQuery
);

router.get(
    '/cellListNR',
    cache12h,
    pgDbStats.cellListNRQuery
);

router.get(
    '/cellListLTE',
    cache12h,
    pgDbStats.cellListLTEQuery
);

router.get(
    '/siteStats',
    pgDbStats.siteStatsQuery
);

router.get(
    '/plmnDailyCellNR',
    cache15m,
    pgDbStats.dailyPlmnCellQueryNR
);

router.get(
    '/plmnDailyCellLTE',
    cache15m,
    pgDbStats.dailyPlmnCellQueryLTE
);


module.exports = router;