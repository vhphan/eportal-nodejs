const express = require('express');
const router = express.Router();
const pgDb = require('../db/PostgresQueries');
const pgDbStats = require('../db/pgQueriesStats');
const pgDbGeo = require('../db/pgQueriesGeo');
const pgJs = require('../db/pgjs/PgJsQueries');

const redis = require("redis");

const asyncHandler = require("../middleware/async");
const download = require("../tools/chartDl");
const {auth} = require("../auth");
const {createProxyMiddleware} = require("http-proxy-middleware");
const {cache15m, cache30m, cacheLongTerm, cache12h} = require("../middleware/redisCache");


router.use(auth('dnb'))

function handler(req, res) {
    return res.send('Hello dnb');
}


router.get('/', handler);
router.get('/cellInfo', cache15m, pgDb.getCellInfo);
router.get('/fullView', cache15m, pgDb.dbFullViewData);
router.get('/changeLog', cache15m, pgDb.getChangeLog);
router.put('/updateNominal', pgDb.updateNominal);
router.put('/updateConfigs', pgDb.updateConfigs);
router.post('/postJob', pgDb.addJob);
router.route('/tabulatorConfig')
    .get(pgDb.getTabulatorConfig)
    .post(pgDb.saveTabulatorConfig);

router.get('/tabulatorData', cache15m, asyncHandler(pgDb.getTabulatorData));

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

router.get(
    '/sampleChart',
    download.getSampleChart())

router.get(
    '/formulas',
    cacheLongTerm,
    pgDbStats.formulasQuery)

router.get(
    '/geojson',
    cache30m,
    pgDbGeo.getCells());

let postgrestProxy = createProxyMiddleware({
    changeOrigin: true,
    prependPath: false,
    target: "http://localhost:3000",
    logLevel: 'debug',
    pathRewrite: {
        '^/node/dnb/pgr': '', // remove base path
    },
});

router.all('/pgr/*', postgrestProxy);

let geoServerProxy = createProxyMiddleware({
    changeOrigin: true,
    prependPath: false,
    target: "http://localhost:8080",
    logLevel: 'debug',
    pathRewrite: {
        '^/node/dnb/geoserver': '', // remove base path
    },
});

router.all('/geoserver/*', geoServerProxy);

router.get('/testQueryPGJS', pgJs.testQuery);

router.get('/nbrRelation', pgJs.getNbrRelation);

module.exports = router;