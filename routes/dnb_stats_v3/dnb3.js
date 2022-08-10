const express = require('express');
const {cache15m, cache12h, cache2h} = require("../../middleware/redisCache");
const asyncHandler = require("../../middleware/async");

const {
    networkDailyStatsLTE,
    clusterDailyStatsLTE,
    regionDailyStatsLTE,
    cellDailyStatsLTE, networkDailyStatsNR, cellDailyStatsNR, cellsList, regionDailyStatsNR, clusterDailyStatsNR,
    cellsListNR, cellsListLTE, customCellListStatsNR, customCellListStatsNR2, customCellListStatsLTE,
    networkDailyPlmnStatsNR, networkDailyPlmnStatsLTE, regionDailyPlmnStatsNR, clusterDailyPlmnStatsNR,
    cellDailyPlmnStatsNR, regionDailyPlmnStatsLTE, clusterDailyPlmnStatsLTE, cellDailyPlmnStatsLTE
} = require("../../db/pgjs/PgJSQueriesStatsV3");

const {logRequest} = require("../../middleware/logger");

const router = express.Router();

router.use(logRequest);

router.get(
    '/networkDailyLTE',
    cache2h,
    asyncHandler(networkDailyStatsLTE)
);

router.get(
    '/regionDailyLTE',
    cache2h,
    asyncHandler(regionDailyStatsLTE)
);


router.get(
    ['/clusterDailyLTE', '/clusterStatsLTE'],
    cache2h,
    asyncHandler(clusterDailyStatsLTE)
);

router.get(
    ['/cellDailyLTE', '/networkDailyLTECell'], // networkDailyLTECell
    cache2h,
    asyncHandler(cellDailyStatsLTE)
);

router.get(
    '/networkDailyNR',
    cache2h,
    asyncHandler(networkDailyStatsNR)
);

router.get(
    '/regionDailyNR',
    cache2h,
    asyncHandler(regionDailyStatsNR)
);

router.get(
    ['/clusterDailyNR', '/clusterStatsNR'],
    cache2h,
    asyncHandler(clusterDailyStatsNR)
);

router.get(
    ['/cellDailyNR', '/networkDailyNRCell'],
    cache2h,
    asyncHandler(cellDailyStatsNR)
);

router.get('/cellListNR', cache12h, asyncHandler(cellsListNR));
router.get('/cellListLTE', cache12h, asyncHandler(cellsListLTE));

router.get('/statsForCustomCellsListNR', asyncHandler(customCellListStatsNR))
router.get('/statsForCustomCellsListNR2', asyncHandler(customCellListStatsNR2))
router.get('/statsForCustomCellsListLTE', asyncHandler(customCellListStatsLTE))

router.get(
    ['/cellsList', 'cellsMapping'],
    cache2h,
    asyncHandler(cellsList)
);

router.get('/checkUser', async (req, res) => {
    res.status(200).send({
        success: true,
    });
})

router.get(
    '/networkDailyPlmnNR',
    cache2h,
    asyncHandler(networkDailyPlmnStatsNR)
);

router.get(
    '/regionDailyPlmnNR',
    cache2h,
    asyncHandler(regionDailyPlmnStatsNR)
);

router.get(
    '/clusterDailyPlmnNR',
    cache2h,
    asyncHandler(clusterDailyPlmnStatsNR)
);

router.get(
    ['/cellDailyPlmnNR', 'plmnDailyCellNR'],
    cache2h,
    asyncHandler(cellDailyPlmnStatsNR)
);

router.get(
    '/networkDailyPlmnLTE',
    cache2h,
    asyncHandler(networkDailyPlmnStatsLTE)
);

router.get(
    '/regionDailyPlmnLTE',
    cache2h,
    asyncHandler(regionDailyPlmnStatsLTE)
);

router.get(
    '/clusterDailyPlmnLTE',
    cache2h,
    asyncHandler(clusterDailyPlmnStatsLTE)
);

router.get(
    '/cellDailyPlmnLTE',
    cache2h,
    asyncHandler(cellDailyPlmnStatsLTE)
);

module.exports = router;