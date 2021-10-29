const express = require('express');
const auth = require("../auth");
const router = express.Router()

router.use(auth('celcom'))

function handler(req, res) {
    return res.send('Hello dnb');
}


router.get('/', handler);

module.exports = router;