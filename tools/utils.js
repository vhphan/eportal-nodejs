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

module.exports = {
    createWatcherProcess,
    renameProp,
    renameProps

}