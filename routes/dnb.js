const express = require('express');
const auth = require("../auth");
const router = express.Router();
const pgDb = require('../db/PostgresQueries');
const mysqlDb = require('../db/MySQLQueries');

const apicache = require('apicache');
const {createListener} = require("../db/utils");
const PostgresBackend = require("../db/PostgresBackend");
//
// let app = express()
let cache = apicache.middleware
//
// app.get('/api/collection/:id?', cache('5 minutes'), (req, res) => {
//   // do some work... this will only occur once per 5 minutes
//   res.json({ foo: 'bar' })
// })

router.use(auth('dnb'))

function handler(req, res) {
    return res.send('Hello dnb');
}


router.get('/', handler);
router.get('/cellInfo',  cache('5 minutes'), pgDb.getCellInfo)
router.get('/fullView',  cache('15 minutes'), pgDb.dbFullViewData)
router.get('/changeLog',  cache('15 minutes'), pgDb.getChangeLog)

router.put('/updateNominal', pgDb.updateNominal)
router.put('/updateConfigs', pgDb.updateConfigs)

module.exports = router;