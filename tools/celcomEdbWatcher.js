const fs = require('fs');
const {spawn} = require("child_process");
const {logger} = require("../middleware/logger");
const {sendEmail} = require("../db/utils");

function watch(dir) {
    logger.info(`start watching ${dir}`);
    const watcher = fs.watch(dir, {
        persistent: true,
        recursive: true
    }, function (event, filename) {
        watcher.close();
        logger.info(`${event} ${filename}`);
        sendEmail(process.env.DEFAULT_EMAIL_ADDRESS, "Celcom EDB Watcher", `${event} ${filename}`);

        const php = spawn("wget", ["-q", "-O", "/dev/null https://cmeportal.eprojecttrackers.com/extract_myedb_file.php"]);
        php.stdout.on("data", data => {
            logger.info(`stdout: ${data}`);
        });
        php.stderr.on("data", data => {
            logger.info(`stderr: ${data}`);
        });
        php.on('error', (error) => {
            logger.error(`error: ${error.message}`);
        });
        php.on("close", code => {
            logger.info(`child process exited with code ${code}`);
        });

        setTimeout(() => {
            watch(dir);
        }, 3600_000)
    });
}

// Celcom my edb /home/eproject/veehuen/myedb/Myedb/Myedb/*.rar
const watchDir = '/home/eproject/veehuen/myedb/Myedb/Myedb';
if (fs.existsSync(watchDir)) {
    console.log(`Directory ${watchDir} exists!`);
    watch(watchDir);
} else {
    console.log(`Directory ${watchDir} not found.`);
}
