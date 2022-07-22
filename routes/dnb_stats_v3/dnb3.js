const express = require('express');
const {cache15m} = require("../../middleware/redisCache");
const asyncHandler = require("../../middleware/async");

const {
    networkDailyStatsLTE,
    clusterDailyStatsLTE,
    regionDailyStatsLTE,
    cellDailyStatsLTE
} = require("../../db/pgjs/PgJSQueriesStatsV3");

const router = express.Router();

router.get(
    '/networkDailyLTE',
    cache15m,
    asyncHandler(networkDailyStatsLTE)
);

router.get(
    '/regionDailyLTE',
    cache15m,
    asyncHandler(regionDailyStatsLTE)
);

router.get(
    '/clusterDailyLTE',
    cache15m,
    asyncHandler(clusterDailyStatsLTE)
);

router.get(
    '/cellDailyLTE',
    cache15m,
    asyncHandler(cellDailyStatsLTE)
);

module.exports = router;