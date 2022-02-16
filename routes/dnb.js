const express = require('express');
const auth = require("../auth");
const router = express.Router();
const pgDb = require('../db/PostgresQueries');
const mysqlDb = require('../db/MySQLQueries');

const apiCache = require('apicache');
const {createListener} = require("../db/utils");
const PostgresBackend = require("../db/PostgresBackend");
const {checkCache, checkCacheMiddleWare} = require("../db/RedisBackend");
const redis = require("redis");
//
// let app = express()
let cache = apiCache.middleware
let cacheWithRedis = apiCache.options({ redisClient: redis.createClient(), debug: true }).middleware;

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
router.get('/cellInfo', cache('5 minutes'), pgDb.getCellInfo);
router.get('/fullView', cache('15 minutes'), pgDb.dbFullViewData);
router.get('/changeLog', cache('15 minutes'), pgDb.getChangeLog);

router.put('/updateNominal', pgDb.updateNominal);
router.put('/updateConfigs', pgDb.updateConfigs);

router.post('/postJob', pgDb.addJob);

router.route('/tabulatorConfig')
    .get(pgDb.getTabulatorConfig)
    .post(pgDb.saveTabulatorConfig);

// router.get('/tabulatorData', checkCacheMiddleWare , pgDb.getTabulatorData);
router.get('/tabulatorData', cacheWithRedis('5 minutes') , pgDb.getTabulatorData);
// router.route('testtest')
//     .get(getHandler)
//     .put(putHandler)
//     .post(postHandler)

module.exports = router;