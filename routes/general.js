const express = require('express');
const router = express.Router()

function handler(req, res) {
    return res.send('Hello General');
}


router.get('/', handler);

module.exports = router;