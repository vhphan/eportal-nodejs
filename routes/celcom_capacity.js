const express = require('express');
const router = express.Router();
const {cache12h, cache15m, cache6h} = require("#src/middleware/redisCache");
const asyncHandler = require("#src/middleware/async");
const {logRequest} = require("#src/middleware/logger");
const {auth} = require("#src/auth");
const {getWorstCellsList, getRegionalCountTrend} = require("#src/db/celcom/capacityQueries");

const operator = 'celcom';
router.use(auth(operator));

router.get('/worstCellsList', asyncHandler(getWorstCellsList));
router.get('/regionalCountTrend', asyncHandler(getRegionalCountTrend));


module.exports = router;