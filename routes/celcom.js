const express = require('express');
const {auth} = require("../auth");
const router = express.Router()
const statsQueries = require('../db/celcom/statsQueries');
const asyncHandler = require("../middleware/async");
const PostgresBackend = require("../db/PostgresBackend");
const {createListeners, createListener, sendEmail} = require("../db/utils");
const sql = require("../db/celcom/PgJsBackend");
const {arrayToCsv, gMap, getFolderContents, downloadZipFile} = require("./utils");
const {excelTestFunc} = require("../db/celcom/statsQueries");
const {cache12h, cache, cache15m} = require("../middleware/redisCache");
const pgDbGeo = require("../db/pgQueriesGeo");
const {getCells, getClusters} = require("../db/celcom/celcomGeoQueries");
const {getReportsPendingHQReview, reviewReport, getReportsBulkApproved} = require("../db/celcom/MySQLQueries");
const pgDb = require("../db/PostgresQueries");
const {getTabulatorDataMySql} = require("../db/MySQLQueries");

router.use(auth('celcom'));
router.locals = {
    lastEmailSentAt: {}
};

const cpg = new PostgresBackend('celcom');
const client = cpg.getClient();

createListener(client, 'new_data', async (data) => {
    console.log(data.payload)
    const updatedTable = data.payload;
    if (['gsm_aggregates_week_columns', 'lte_aggregates_week_columns'].includes(updatedTable)) {
        const tech = updatedTable.substring(0, 4);
        const message = `Aggregation completed for ${tech}`;
        sendEmail(process.env.CELCOM_EMAILS, `Auto Messaging: Aggregation completed [${tech}]`, message);
        return;
    }

    const message = `New data has been added to ${updatedTable} in database. Aggregation are being performed.`;
    if (router.locals.lastEmailSentAt[updatedTable] && (Date.now() - router.locals.lastEmailSentAt[updatedTable]) < 1000 * 60 * 30) {
        return;
    }
    7
    sendEmail(process.env.CELCOM_EMAILS, 'Auto Messaging: New Data Processed', message);
    router.locals.lastEmailSentAt[updatedTable] = new Date();

});

router.get('/', function (req, res) {
    return res.send('Hello Celcom 321');
});
router.get('/2', function (req, res) {
    return res.send('Hello Core2');
});

router.get('/lte-stats/aggregate', asyncHandler(statsQueries.getAggregatedStats('LTE')));
router.get('/lte-stats/aggregateWeek', asyncHandler(statsQueries.getAggregatedStatsWeek('LTE')));
router.get('/lte-stats/cellStats', asyncHandler(statsQueries.getCellStats('LTE')));
router.get('/lte-stats/cellMapping', asyncHandler(statsQueries.getCellMapping('LTE')));
router.get('/lte-stats/groupedCellsDaily', asyncHandler(statsQueries.getGroupedCellsStats('LTE', 'get')));
router.post('/lte-stats/groupedCellsDaily', asyncHandler(statsQueries.getGroupedCellsStats('LTE', 'post')));

router.get('/gsm-stats/aggregate', asyncHandler(statsQueries.getAggregatedStats('GSM')));
router.get('/gsm-stats/aggregateWeek', asyncHandler(statsQueries.getAggregatedStatsWeek('GSM')));
router.get('/gsm-stats/cellStats', asyncHandler(statsQueries.getCellStats('GSM')));
router.get('/gsm-stats/cellMapping', asyncHandler(statsQueries.getCellMapping('GSM')));
router.get('/gsm-stats/groupedCellsDaily', asyncHandler(statsQueries.getGroupedCellsStats('GSM', 'get')));
router.post('/gsm-stats/groupedCellsDaily', asyncHandler(statsQueries.getGroupedCellsStats('GSM', 'post')));
router.get('/all-stats/cellMapping', asyncHandler(statsQueries.getCellMapping('ALL')));


router.get('/gsm-stats/clusterStats', asyncHandler(statsQueries.getClusterStats('GSM')));
router.get('/lte-stats/clusterStats', asyncHandler(statsQueries.getClusterStats('LTE')));


router.get('/googleMap', cache('2 minutes'), gMap())
router.post('/testExcel', asyncHandler(excelTestFunc));

router.get(
    '/clustersPolygons',
    cache12h,
    asyncHandler(getClusters)
)

router.get(
    '/geojson',
    cache12h,
    asyncHandler(getCells));

router.get('/ssoReportsPendingHQReview', asyncHandler(getReportsPendingHQReview));

router.put('/ssoReportsPendingHQReview/:reportId', asyncHandler(reviewReport('single')));

router.get('/ssoReportsBulkApproved', asyncHandler(getReportsBulkApproved));

router.put('/ssoReportsBulkApproved', asyncHandler(reviewReport('multiple')));

// router.post('/upload-test-file', asyncHandler(async (req, res) => {
//         if (!req.files) {
//             return res.status(501).json({
//                 status: false,
//                 message: 'No file uploaded'
//             });
//         } else {
//             //Use the name of the input field (i.e. "testFile") to retrieve the uploaded file
//             let testFile = req.files.test;
//
//             //Use the mv() method to place the file in upload directory (i.e. "uploads")
//             testFile.mv(fileRepo.celcom + '/' + testFile.name);
//
//             //send response
//             return res.status(200).json({
//                 status: true,
//                 message: 'File is uploaded',
//                 data: {
//                     name: testFile.name,
//                     mimetype: testFile.mimetype,
//                     size: testFile.size
//                 }
//             });
//         }
//     }));
router.get('/tabulatorData', cache15m,
    asyncHandler(getTabulatorDataMySql('celcom')));

router.get('/statsFiles', asyncHandler(async (req, res) => {
    const files = getFolderContents('/home2/eproject/celcom/stats_files');
    const filesWithLinks = files.map(file => ({
        ...file,
        link: `https://api.eprojecttrackers.com/node/celcom/statsFiles/${file.name}`
    }))
    return res.json(filesWithLinks);
}));

router.get('/statsFiles/:fileName', asyncHandler(
    downloadZipFile('celcom', 'stats_files')
));

module.exports = router;