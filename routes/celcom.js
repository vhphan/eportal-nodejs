const express = require('express');
const {auth} = require("../auth");
const router = express.Router()
const statsQueries = require('../db/celcom/statsQueries');
const asyncHandler = require("../middleware/async");

router.use(auth('celcom'))

function handler(req, res) {
    return res.send('Hello Celcom');
}

function handler2(req, res) {
    return res.send('Hello Core2');
}



router.get('/', handler);
router.get('/2', handler2);

router.get('/lte-stats/aggregate', asyncHandler(statsQueries.getAggregatedStats));
router.get('/lte-stats/aggregateWeek', asyncHandler(statsQueries.getAggregatedStatsWeek));
router.get('/lte-stats/cellStats', asyncHandler(statsQueries.getCellStats));
router.get('/lte-stats/cellMapping', asyncHandler(statsQueries.getCellMapping));

module.exports = router;