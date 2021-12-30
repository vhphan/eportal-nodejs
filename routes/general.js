const express = require('express');
const router = express.Router()

function handler(req, res) {
    return res.send('Hello General');
}


router.get('/', handler);

router.get('/socket', function(req,res) {
  res.sendfile('public/socket.html');
});

module.exports = router;