const CronJob = require("node-cron");
const MySQLBackend = require("../db/MySQLBackend");
const {generateStrongPassword} = require("../tools/utils");
const {sendEmail} = require("../db/utils");
const {logger} = require("../middleware/logger");


const resetSomeUsers = async (numOfUsers=2) => {
    const mysqlCelcom = new MySQLBackend('celcom');
    const sqlQuery = `SELECT UserID, Email FROM eproject_cm.tbluser WHERE Active = 1 and DATEDIFF(NOW(), PasswordReset) > 30 ORDER BY RAND() LIMIT ?`;
    // const sqlQuery = `SELECT UserID, Email FROM eproject_cm.tbluser WHERE Email='vee.huen.phan@ericsson.com' LIMIT ?`;
    const [rows, fields] = await mysqlCelcom.query(sqlQuery, [numOfUsers]);
    const updateResults = rows.map(async row => {
        const userId = row['UserID'];
        const email = row['Email'];
        const updateSql = `UPDATE eproject_cm.tbluser
                           SET Password     = ?,
                               PasswordReset=NOW()
                           WHERE UserID = ?`;
        const strongPassword = generateStrongPassword();
        const updateParams = [strongPassword, userId];
        const result = await mysqlCelcom.query(updateSql, updateParams);
        if (result[0] && result[0].affectedRows > 0) {
            logger.info(`Password reset for user ${userId} with email ${email}`);
            sendEmail(email, 'Password Reset', `Your password has been reset. New password is ${strongPassword}`).then(() => {
                logger.info(`Email sent to ${email} for password reset`)
            });
        }
    })
}

const initScheduledJobsForCelcom = () => {
    // create cron job for every day at 7:00 AM
    const cronString = "0 7 * * *";
    CronJob.schedule(cronString, async () => {
        logger.info("Running scheduled job for Celcom at 7:00 AM");
        await resetSomeUsers(2);
    }).start();
};

module.exports = {
    resetSomeUsers,
    initScheduledJobsForCelcom
}