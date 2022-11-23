const express = require('express');
const {auth} = require("../auth");
const router = express.Router();
const {cache15m, cache30m, cacheLongTerm, cache12h} = require("../middleware/redisCache");
const {
    clusterDailyStatsNR, testQuery, clusterDailyStatsLTE, customCellListStatsNR, customCellListStatsNR2,
    customCellListStatsLTE, customCellListStatsLTE2, clusterHourlyStatsNR, clusterHourlyStatsLTE, updateUserID
} = require("../db/pgjs/PgJSQueriesStats");
const asyncHandler = require("../middleware/async");
const sql = require("../db/pgjs/PgJsBackend");
const pgDbGeo = require("../db/pgQueriesGeo");
const {getRaster} = require("../db/pgjs/PgJsQueries14");
const {testRunPython} = require("./utils");

router.get('/', testQuery)
router.get('/clusterStatsNR', asyncHandler(clusterDailyStatsNR))
router.get('/clusterStatsLTE', asyncHandler(clusterDailyStatsLTE))
router.get('/clusterList', async (request, response) => {
    const results = await sql`
    SELECT DISTINCT "Cluster_ID"
    FROM dnb.rfdb.cell_mapping as t1
        INNER JOIN dnb.stats_v3.cell_list_nr as t2
        on t1."Cellname" = t2."object"
        WHERE "Cluster_ID" is not null
    ORDER BY "Cluster_ID";`
    response.status(200).json(results);
});

router.get('/statsForCustomCellsListNR', asyncHandler(customCellListStatsNR))
router.get('/statsForCustomCellsListNR2', asyncHandler(customCellListStatsNR2))
router.get('/statsForCustomCellsListLTE', asyncHandler(customCellListStatsLTE))
router.get('/statsForCustomCellsListLTE2', asyncHandler(customCellListStatsLTE2))

router.get(
    '/clustersPolygons',
    cache12h,
    asyncHandler(pgDbGeo.getClusters)
)

router.get('/clusterStatsHourlyNR', asyncHandler(clusterHourlyStatsNR));
router.get('/clusterStatsHourlyLTE', asyncHandler(clusterHourlyStatsLTE));
router.post('/updateDashboardUser', asyncHandler(updateUserID));

router.get('/rasters/:raster', asyncHandler(getRaster));

router.get('/testRunPython/:arg1/:arg2', asyncHandler(testRunPython));

module.exports = router;