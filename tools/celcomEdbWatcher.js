const fs = require('fs');
const {spawn, exec} = require("child_process");
const {logger} = require("../middleware/logger");
const {sendEmail} = require("../db/utils");
const watch = require("node-watch");


const oneMinute = 60_000;

function genericCallBack(evt, name, dir, cmdLine, warmUpPeriodMinutes = 10, debouncePeriodMinutes = 60) {
    setTimeout(() => {
        exec(cmdLine,
            (err, stdout, stderr) => {
                if (err) {
                    logger.error(`exec error: ${err}`);
                    sendEmail(process.env.DEFAULT_EMAIL_ADDRESS, `${arguments.callee.name} ERROR`, `${err}`);
                    return;
                }
                if (stderr) {
                    logger.info(`stderr: ${stderr}`);
                    return;
                }
                logger.info(`stdout: ${stdout}`);
                sendEmail(process.env.DEFAULT_EMAIL_ADDRESS, `${arguments.callee.name} running..`, `${evt} ${name}`);
            }
        )
    }, warmUpPeriodMinutes * oneMinute)

    setTimeout(() => {
        watch(dir);
    }, debouncePeriodMinutes * oneMinute)
}

function edbCallBack(evt, name, dir) {
    const cmd = 'wget -q -O /dev/null https://cmeportal.eprojecttrackers.com/extract_myedb_file.php';
    genericCallBack(evt, name, dir, cmd, 45);
}

function watchFolder(dir, callBackFunc) {

    if (!fs.existsSync(dir)) {
        logger.info(`Directory ${dir} not found. Not watching.`);
        return;
    }
    logger.info(`Directory ${dir} exists!`);
    logger.info(`start watching ${dir}`);
    const watcher = watch(dir, {recursive: true}, function (evt, name) {
        watcher.close();
        logger.info(`${name} changed.`);
        logger.info(`${evt} ${name}`);
        sendEmail(process.env.DEFAULT_EMAIL_ADDRESS, "Celcom edb Folder Watcher", `${evt} ${name}`);
        callBackFunc(evt, name, dir);
    });
}

function lteDataCallBack(evt, name, dir) {
    const cmd = 'cd /home/eproject/celcom && /home/eproject/anaconda3/envs/dnb/bin/python -m scripts.lte_processor';
    genericCallBack(evt, name, dir, cmd);
}

function gsmDataCallBack(evt, name, dir) {
    const cmd = 'cd /home/eproject/celcom && /home/eproject/anaconda3/envs/dnb/bin/python -m scripts.gsm_processor';
    genericCallBack(evt, name, dir, cmd);
}


// Celcom my edb /home/eproject/public_html/cmeportal/Myedb/Myedb/*.rar
watchFolder('/home/eproject/public_html/cmeportal/Myedb/Myedb', edbCallBack);

// watchFolder('/home/eproject/eproject_bo/v3', lteDataCallBack);
// watchFolder('/home/eproject/eproject_bo/2G_v2', gsmDataCallBack);

module.exports = {
    watchFolder,
    genericCallBack,
}