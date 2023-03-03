const express = require('express');
const router = express.Router();
const {cache12h, cache15m, cache6h} = require("#src/middleware/redisCache");
const asyncHandler = require("#src/middleware/async");
const {logRequest} = require("#src/middleware/logger");
const {auth} = require("#src/auth");
const {getWorstCellsList, getRegionalCountTrend, getIssueCountTrend, getAgingCountTrend, getMaxYearWeek,
    getAvailableYearWeeks, getOverallCountAndPercentageTrend
} = require("#src/db/celcom/capacityQueries");
const pgDb = require("#src/db/PostgresQueries");

const operator = 'celcom';
router.use(auth(operator));

router.get('/regionalCountTrend', asyncHandler(getRegionalCountTrend));

router.get('/worstCellsList', asyncHandler(getWorstCellsList));
router.get('/getTabulatorData', asyncHandler(pgDb.getTabulatorData(operator, 14)));

router.get('/getIssueCountTrend', asyncHandler(getIssueCountTrend));
router.get('/getAgingCountTrend', asyncHandler(getAgingCountTrend));

router.get('/getMaxYearWeek', asyncHandler(getMaxYearWeek));
router.get('/getAvailableYearWeeks', asyncHandler(getAvailableYearWeeks));
router.get('/getOverallCountAndPercentageTrend', asyncHandler(getOverallCountAndPercentageTrend));

module.exports = router;