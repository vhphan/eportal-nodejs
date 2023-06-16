const express = require('express');
const router = express.Router();
const {cache12h, cache15m, cache6h} = require("#src/middleware/redisCache");
const asyncHandler = require("#src/middleware/async");
const {logRequest} = require("#src/middleware/logger");
const {auth} = require("#src/auth");
const {
    getWorstCellsList,
    getMaxYearWeek,
    getAvailableYearWeeks, getRegionalCountTrend, getAgingCountTrend,
} = require("#src/db/celcom/capacityQueries");
const pgDb = require("#src/db/PostgresQueries");

const operator = 'celcom';

router.use(auth(operator));

// Dashboard Page 1
router.get('/regionalCountTrend', cache12h, asyncHandler(getRegionalCountTrend));
router.get('/agingCountTrend', cache12h, asyncHandler(getAgingCountTrend));
router.get('/worstCellsList', cache12h, asyncHandler(getWorstCellsList));

router.get('/getTabulatorData', asyncHandler(pgDb.getTabulatorData(operator, 14)));

router.get('/getMaxYearWeek', asyncHandler(getMaxYearWeek));

router.get('/getAvailableYearWeeks', asyncHandler(getAvailableYearWeeks));


module.exports = router;