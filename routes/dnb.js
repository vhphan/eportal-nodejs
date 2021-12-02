const express = require('express');
const auth = require("../auth");
const router = express.Router();
const pg_db = require('../db/PostgresQueries');

router.use(auth('dnb'))

function handler(req, res) {
    return res.send('Hello dnb');
}


router.get('/', handler);
router.get('/cellInfo', pg_db.getCellInfo)

module.exports = router;