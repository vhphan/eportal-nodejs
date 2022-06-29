const express = require('express');
const {auth} = require("../auth");
const router = express.Router();
const {cache15m, cache30m, cacheLongTerm, cache12h} = require("../middleware/redisCache");
const {clusterDailyStatsNR, testQuery, clusterDailyStatsLTE} = require("../db/pgjs/PgJSQueriesStats");
const asyncHandler = require("../middleware/async");
const sql = require("../db/pgjs/PgJsBackend");
const pgDbGeo = require("../db/pgQueriesGeo");

router.use(auth('dnb'))
router.get('/', testQuery)
router.get('/clusterStatsNR', asyncHandler(clusterDailyStatsNR))
router.get('/clusterStatsLTE', asyncHandler(clusterDailyStatsLTE))
router.get('/clusterList', async(request, response)=>{
    const results = await sql`
    SELECT DISTINCT "Cluster_ID"
    FROM dnb.rfdb.cell_mapping as t1
        INNER JOIN dnb.stats.cell_list_nr as t2
        on t1."Cellname" = t2."object"
        WHERE "Cluster_ID" is not null
    ORDER BY "Cluster_ID";`
    response.status(200).json(results);
});

router.get(
    '/clustersPolygons',
    cache12h,
    asyncHandler(pgDbGeo.getClusters)
)


module.exports = router;