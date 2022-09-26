const express = require('express');
const {cache12h, cache15m, cache6h} = require("../middleware/redisCache");
const asyncHandler = require("../middleware/async");
const {logRequest} = require("../middleware/logger");
const {getProjects, getProcesses, getSimplifiedE2E} = require("../db/celcom/ePortalQueries");

const router = express.Router();

router.use(logRequest);

router.get('/test', cache15m, asyncHandler(async (request, response) => {
    response.status(200).json({success: true});
}));

router.get('/projects', cache6h, asyncHandler(getProjects));
router.get('/processes', cache6h, asyncHandler(getProcesses));
router.get('/simplified-e2e', cache15m, asyncHandler(getSimplifiedE2E));

module.exports = router;