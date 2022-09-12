const express = require('express');
const {cache12h, cache15m, cache6h} = require("../../middleware/redisCache");
const asyncHandler = require("../../middleware/async");

const {
    networkDailyStatsLTE,
    clusterDailyStatsLTE,
    regionDailyStatsLTE,
    cellDailyStatsLTE,
    networkDailyStatsNR,
    cellDailyStatsNR,
    cellsList,
    regionDailyStatsNR,
    clusterDailyStatsNR,
    cellsListNR,
    cellsListLTE,
    customCellListStatsNR,
    customCellListStatsNR2,
    customCellListStatsLTE,
    networkDailyPlmnStatsNR,
    networkDailyPlmnStatsLTE,
    regionDailyPlmnStatsNR,
    clusterDailyPlmnStatsNR,
    cellDailyPlmnStatsNR,
    regionDailyPlmnStatsLTE,
    clusterDailyPlmnStatsLTE,
    cellDailyPlmnStatsLTE,
    clusterStatsAggregatedNR,
    clusterStatsAggregatedLTE,
} = require("../../db/pgjs/DailyQueriesStatsV3");

const {logRequest} = require("../../middleware/logger");

const {
    networkHourlyStatsNR,
    regionHourlyStatsNR,
    clusterHourlyStatsNR,
    cellHourlyStatsNR,
    networkHourlyStatsLTE,
    clusterHourlyStatsLTE,
    regionHourlyStatsLTE,
    cellHourlyStatsLTE,
    customCellListHourlyStatsNR,
    customCellListHourlyStatsNR2,
    customCellListHourlyStatsLTE,
    networkHourlyPlmnStatsNR, networkHourlyPlmnStatsLTE
} = require("../../db/pgjs/HourlyQueriesStatsV3");

const router = express.Router();

router.use(logRequest);

router.get('/networkDailyLTE', cache15m, asyncHandler(networkDailyStatsLTE));
router.get('/regionDailyLTE', cache15m, asyncHandler(regionDailyStatsLTE));
router.get(['/clusterDailyLTE', '/clusterStatsLTE'], cache15m, asyncHandler(clusterDailyStatsLTE));
router.get(['/cellDailyLTE', '/networkDailyLTECell'], // networkDailyLTECell
    cache15m, asyncHandler(cellDailyStatsLTE));
router.get('/networkDailyNR', cache15m, asyncHandler(networkDailyStatsNR));
router.get('/regionDailyNR', cache15m, asyncHandler(regionDailyStatsNR));
router.get(['/clusterDailyNR', '/clusterStatsNR'], cache15m, asyncHandler(clusterDailyStatsNR));
router.get(['/cellDailyNR', '/networkDailyNRCell'], cache15m, asyncHandler(cellDailyStatsNR));
router.get('/cellListNR', cache12h, asyncHandler(cellsListNR));
router.get('/cellListLTE', cache12h, asyncHandler(cellsListLTE));

router.get('/statsForCustomCellsListNR', asyncHandler(customCellListStatsNR))
router.get('/statsForCustomCellsListNR2', asyncHandler(customCellListStatsNR2))
router.get('/statsForCustomCellsListLTE', asyncHandler(customCellListStatsLTE))

router.get(['/cellsList', 'cellsMapping'], cache15m, asyncHandler(cellsList));
router.get('/checkUser', async (req, res) => {
    res.status(200).send({
        success: true,
    });
})

router.get(['/networkDailyPlmnNR', '/plmnDailyNR'], cache15m, asyncHandler(networkDailyPlmnStatsNR));
router.get('/regionDailyPlmnNR', cache15m, asyncHandler(regionDailyPlmnStatsNR));
router.get('/clusterDailyPlmnNR', cache15m, asyncHandler(clusterDailyPlmnStatsNR));
router.get(['/cellDailyPlmnNR', '/plmnDailyCellNR'], cache15m, asyncHandler(cellDailyPlmnStatsNR));

// PLMN LTE
router.get(['/networkDailyPlmnLTE', '/plmnDailyLTE'], cache15m, asyncHandler(networkDailyPlmnStatsLTE));
router.get('/regionDailyPlmnLTE', cache15m, asyncHandler(regionDailyPlmnStatsLTE));
router.get('/clusterDailyPlmnLTE', cache15m, asyncHandler(clusterDailyPlmnStatsLTE));
router.get(['/cellDailyPlmnLTE', '/plmnDailyCellLTE'], cache15m, asyncHandler(cellDailyPlmnStatsLTE));

//==========================================================================
//HOURLY
//==========================================================================
router.get('/networkHourlyNR', cache15m, asyncHandler(networkHourlyStatsNR));
router.get('/regionHourlyNR', cache15m, asyncHandler(regionHourlyStatsNR));
router.get(['/clusterHourlyNR', '/clusterStatsHourlyNR'], cache15m, asyncHandler(clusterHourlyStatsNR));
router.get(['/cellHourlyNR', '/networkHourlyNRCell'], cache15m, asyncHandler(cellHourlyStatsNR));
router.get('/networkHourlyLTE', cache15m, asyncHandler(networkHourlyStatsLTE));
router.get('/regionHourlyLTE', cache15m, asyncHandler(regionHourlyStatsLTE));
router.get(['/clusterHourlyLTE', '/clusterStatsHourlyLTE'], cache15m, asyncHandler(clusterHourlyStatsLTE));
router.get(['/cellHourlyLTE', '/networkHourlyLTECell'], cache15m, asyncHandler(cellHourlyStatsLTE));
router.get(['/clusterCustomDatesNR'], cache6h, asyncHandler(clusterStatsAggregatedNR));
router.get(['/clusterCustomDatesLTE'], cache6h, asyncHandler(clusterStatsAggregatedLTE));
router.get('/statsForCustomCellsListNRHourly', asyncHandler(customCellListHourlyStatsNR))
router.get('/statsForCustomCellsListNR2Hourly', asyncHandler(customCellListHourlyStatsNR2))
router.get('/statsForCustomCellsListLTEHourly', asyncHandler(customCellListHourlyStatsLTE))

router.get(['/networkHourlyPlmnNR', '/plmnHourlyNR'], cache15m, asyncHandler(networkHourlyPlmnStatsNR));
router.get(['/networkHourlyPlmnLTE', '/plmnHourlyLTE'], cache15m, asyncHandler(networkHourlyPlmnStatsLTE));

module.exports = router;