const express = require('express');
const auth = require("../auth");
const router = express.Router()

router.use(auth('celcom'))

function handler(req, res) {
    return res.send('Hello Celcom');
}

function handler2(req, res) {
    return res.send('Hello Core2');
}

router.get('/', handler);
router.get('/2', handler2);

module.exports = router;