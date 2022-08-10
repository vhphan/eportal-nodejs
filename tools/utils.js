const fs = require("fs");
const {logger} = require("../middleware/logger");

function logWatcher(watcher) {
    watcher.stdout.on('data', function (data) {
        logger.info("stdout: " + data);
    });

    watcher.stderr.on('data', function (data) {
        logger.info("stderr: " + data);
    });

    watcher.on('close', function (code) {
        logger.info("child process exited with code " + code);
    });
}

function createWatcherProcess() {
    logger.info("start watching");
    const spawn = require('child_process').spawn;

    const watcher1 = spawn('node', ['./tools/celcomEdbWatcher.js']);
    logWatcher(watcher1);

    const watcher2 = spawn('node', ['./tools/dnb/netanDataDnbWatcher.js']);
    logWatcher(watcher2);
}

const renameProp = (
    oldProp,
    newProp,
{ [oldProp]: valueOfOldProp, ...others }
) => ({
    [newProp]: valueOfOldProp,
    ...others
});

// function to rename multiple properties in an object\
function renameProps(obj, oldProps, newProps) {
    return Object.keys(obj).reduce((acc, key) => {
        const newKey = oldProps.indexOf(key) > -1 ? newProps[oldProps.indexOf(key)] : key;
        acc[newKey] = obj[key];
        return acc;
    }, {});
}

const unless = function(middleware, ...paths) {
  return function(req, res, next) {
    const pathCheck = paths.some(path => path === req.path);
    pathCheck ? next() : middleware(req, res, next);
  };
};

module.exports = {
    createWatcherProcess,
    renameProp,
    renameProps,
    unless
}