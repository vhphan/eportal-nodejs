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
const asyncHandler = require("../middleware/async");


let cache = apiCache.middleware
let cacheWithRedis = apiCache.options({redisClient: redis.createClient(), debug: true}).middleware;


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
const cache30m = cacheWithRedis('30 minutes', onlyStatus200);
const cache12h = cacheWithRedis('12 hours', onlyStatus200);

router.get('/tabulatorData', cache15m, pgDb.getTabulatorData);

// router.route('testtest')
//     .get(getHandler)
//     .put(putHandler)
//     .post(postHandler)

router.get(
    '/networkDailyNR',
    cache15m,
    asyncHandler(pgDbStats.dailyNetworkQueryNR)
);

router.get(
    '/networkDailyLTE',
    cache15m,
    asyncHandler(pgDbStats.dailyNetworkQueryLTE)
);

router.get(
    '/plmnDailyNR',
    cache15m,
    asyncHandler(pgDbStats.dailyPlmnQueryNR)
);

router.get(
    '/plmnDailyLTE',
    cache15m,
    asyncHandler(pgDbStats.dailyPlmnQueryLTE)
);

router.get(
    '/siteList',
    cache12h,
    asyncHandler(pgDbStats.siteListQuery)
);

router.get(
    '/cellListNR',
    cache12h,
    asyncHandler(pgDbStats.cellListNRQuery)
);

router.get(
    '/cellListLTE',
    cache12h,
    asyncHandler(pgDbStats.cellListLTEQuery)
);

router.get(
    '/siteStats',
    asyncHandler(pgDbStats.siteStatsQuery)
);

router.get(
    '/plmnDailyCellNR',
    cache15m,
    asyncHandler(pgDbStats.dailyPlmnCellQueryNR)
);

router.get(
    '/plmnDailyCellLTE',
    cache15m,
    asyncHandler(pgDbStats.dailyPlmnCellQueryLTE)
);

router.get(
    '/networkDailyNRCell',
    cache15m,
    asyncHandler(pgDbStats.dailyNetworkCellQueryNR)
)

router.get(
    '/networkDailyLTECell',
    cache15m,
    asyncHandler(pgDbStats.dailyNetworkCellQueryLTE)
)


router.get(
    '/networkHourlyLTE',
    cache30m,
    asyncHandler(pgDbStats.hourlyNetworkQueryLTE)
)

router.get(
    '/networkHourlyLTECell',
    cache30m,
    asyncHandler(pgDbStats.hourlyNetworkCellQueryLTE)
)

router.get(
    '/networkHourlyNR',
    cache30m,
    asyncHandler(pgDbStats.hourlyNetworkQueryNR)
)

router.get(
    '/networkHourlyNRCell',
    cache30m,
    asyncHandler(pgDbStats.hourlyNetworkCellQueryNR)
)


router.get(
    '/plmnHourlyLTE',
    cache30m,
    asyncHandler(pgDbStats.hourlyPlmnQueryLTE)
)

router.get(
    '/plmnHourlyCellLTE',
    cache30m,
    asyncHandler(pgDbStats.hourlyPlmnCellQueryLTE)
)

router.get(
    '/plmnHourlyNR',
    cache30m,
    asyncHandler(pgDbStats.hourlyPlmnQueryNR)
)

router.get(
    '/plmnHourlyCellNR',
    cache30m,
    asyncHandler(pgDbStats.hourlyPlmnCellQueryNR)
)

module.exports = router;