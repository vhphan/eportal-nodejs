const express = require('express');
const router = express.Router();
const pgDb = require('../db/PostgresQueries');
const pgDbStats = require('../db/pgQueriesStats');
const pgDbGeo = require('../db/pgQueriesGeo');
const pgJs = require('../db/pgjs/PgJsQueries');
const compression = require('compression');
const redis = require("redis");
const apiCache = require('apicache')
let cache = apiCache.middleware
const asyncHandler = require("../middleware/async");
const download = require("../tools/chartDl");
const {auth} = require("../auth");
const {createProxyMiddleware} = require("http-proxy-middleware");
const {cache15m, cache30m, cacheLongTerm, cache12h} = require("../middleware/redisCache");
const needle = require("needle");


router.use(auth('dnb'))
// router.use(compression);

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
    cache12h,
    asyncHandler(pgDbGeo.getCells));

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

const gMapUrl = `https://maps.googleapis.com/maps/api/js`
router.get('/googleMap', cache('2 minutes'), async (req, res, next) => {
  try {
    const params = new URLSearchParams({
      key: process.env.GOOGLE_KEY,
        libraries: 'places,visualization,geometries',
        v: 'quarterly'
    })

    const apiRes = await needle('get', `${gMapUrl}?${params}`)
    const data = apiRes.body

    // Log the request to the public API
    if (process.env.NODE_ENV !== 'production') {
      console.log(`REQUEST: ${gMapUrl}?${params}`)
    }

    res.status(200).send(data)
  } catch (error) {
    next(error)
  }
})

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

router.get('/checkUser', async(req, res) => {
    res.status(200).send({
        success: true,
    });
})

module.exports = router;