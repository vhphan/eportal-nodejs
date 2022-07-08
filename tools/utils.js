const fs = require("fs");

function createWatcherProcess() {
    const spawn = require('child_process').spawn;
    const watcher = spawn('node', ['./tools/celcomEdbWatcher.js']);

    watcher.stdout.on('data', function (data) {
        console.log("stdout: " + data);
    });

    watcher.stderr.on('data', function (data) {
        console.log("stderr: " + data);
    });

    watcher.on('close', function (code) {
        console.log("child process exited with code " + code);
    });
}


module.exports = {
    createWatcherProcess
}