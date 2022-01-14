const express = require('express');
const router = express.Router()
const fs = require('fs');

function handler(req, res) {
    return res.send('Hello General');
}

function verHandler(req, res) {
    return res.send(process.versions.node);
}

function testHandler(req, res) {
    console.log(new Buffer(req.headers.authorization.split(" ")[1], 'base64').toString())
    return res.json({success: true})
}

function testGeoJSON(req, res) {
    console.log(req);

    if (req.headers.authorization) {
        console.log(new Buffer(req.headers.authorization.split(" ")[1], 'base64').toString());
    }

    res.setHeader('Content-Type', 'application/json');
    fs.createReadStream(__dirname + '/../test_files/cell_g18_CENTRAL_tiny.GeoJSON').pipe(res);
}

router.get('/', handler);
router.get('/version', verHandler);
router.get('/socket', function (req, res) {
    res.sendfile('static/socket.html');
});
router.get('/test', testHandler);
router.get('/testGeoJSON', testGeoJSON);

module.exports = router;