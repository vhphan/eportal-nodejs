const express = require('express');
const {auth} = require("../auth");
const router = express.Router()
const statsQueries = require('../db/celcom/statsQueries');
const asyncHandler = require("../middleware/async");
const PostgresBackend = require("../db/PostgresBackend");
const {createListeners, createListener, sendEmail} = require("../db/utils");

router.use(auth('celcom'))

const cpg = new PostgresBackend('celcom');
const client = cpg.getClient();

createListener(client, 'new_data', async (data) => {
    console.log(data.payload)
    const updatedTable = data.payload;
    if (['gsm_aggregates_week', 'lte_aggregates_week'].includes(updatedTable)) {
        const tech = updatedTable.substring(0, 4);
        const message = `Aggregation completed for ${tech}`;
        sendEmail('beng.tat.lim@ericsson.com, louis.lee.shao.jun@ericsson.com, vee.huen.phan@ericsson.com', `Auto Messaging: Aggregation completed [${tech}]`, message);
        return;
    }
    const message = `New data has been added to ${updatedTable} in database. Aggregation are being performed.`;
    sendEmail('beng.tat.lim@ericsson.com, louis.lee.shao.jun@ericsson.com, vee.huen.phan@ericsson.com', 'Auto Messaging: New Data Processed', message);
});

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