const express = require('express');
const auth = require("../auth");
const router = express.Router();
const pgDb = require('../db/PostgresQueries');
const mysqlDb = require('../db/MySQLQueries');

router.use(auth('dnb'))

function handler(req, res) {
    return res.send('Hello dnb');
}


router.get('/', handler);
router.get('/cellInfo', pgDb.getCellInfo)
router.put('/updateNominal', pgDb.updateNominal)
router.put('/updateConfigs', pgDb.updateConfigs)

module.exports = router;