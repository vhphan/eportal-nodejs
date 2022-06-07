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

router.get('/lte-stats/aggregate', asyncHandler(statsQueries.getAggregatedStats('LTE')));
router.get('/lte-stats/aggregateWeek', asyncHandler(statsQueries.getAggregatedStatsWeek('LTE')));
router.get('/lte-stats/cellStats', asyncHandler(statsQueries.getCellStats('LTE')));
router.get('/lte-stats/cellMapping', asyncHandler(statsQueries.getCellMapping('LTE')));
router.get('/lte-stats/groupedCellsDaily', asyncHandler(statsQueries.getGroupedCellsStats('LTE')));

router.get('/gsm-stats/aggregate', asyncHandler(statsQueries.getAggregatedStats('GSM')));
router.get('/gsm-stats/aggregateWeek', asyncHandler(statsQueries.getAggregatedStatsWeek('GSM')));
router.get('/gsm-stats/cellStats', asyncHandler(statsQueries.getCellStats('GSM')));
router.get('/gsm-stats/cellMapping', asyncHandler(statsQueries.getCellMapping('GSM')));
router.get('/gsm-stats/groupedCellsDaily', asyncHandler(statsQueries.getGroupedCellsStats('GSM')));

module.exports = router;