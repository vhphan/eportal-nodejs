const MySQLBackend = require("../../db/MySQLBackend");
const {getCacheKeyValue} = require("../RedisBackend");
const {logger} = require("../../middleware/logger");
const {getUser} = require("../../auth");
const {getSqlResults} = require("./utils");

// const mysql = new MySQLBackend('celcom');
const mysql = new MySQLBackend('celcom_test');


const getReportsPendingHQReview = async (request, response) => {
    const sqlQuery = `
                    SELECT a.SiteInfoID,
                    a.ReportID,
                    d.Region,
                    d.\`PO\`,
                    d.\`Project\`,
                    a.\`Report Type\`,
                    d.\`Technology\`,
                    a.\`Stage\`,
                    a.\`Report Name\`,
                    d.\`Technology Base Acceptance Cluster\`,
                    d.\`Site Name\`,
                    d.\`LocationID\`,
                    a.Submission,
                    a.\`Submission Date\`,
                    d.Owner,
                    a.\`Prepared By\`,
                    b.\`Reviewed By\`,
                    b.\`Review Date\`,
                    d.\`KPI COA Submitted\`,
                    d.\`HOU KPI COA Approved\`,
                    d.\`HOD KPI COA Approved\`,
                    a.\`Report Path\`,
                    a.\`Status\`,
                    b.\`Comment\`,
                    a.\`Submitted to Regional\`,
                    d.\`CBOClusterName\`,
                    d.\`SiteInfoID\`,
                    a.\`Regional Review Done\`,
                    a.\`Regional Review Status\`,
                    a.\`Punch List Status\`,
                    d.\`Main Activity\`,
                    d.\`Process\`
                    ,
                    \`Opti Allocation\`
                    FROM (SELECT r1.*
                    FROM tblreport as r1
                           INNER JOIN eproject_cm.bulk_approve as ba
                                      on r1.SiteInfoID = ba.SiteInfoID) as a
                     LEFT JOIN (SELECT ReportID, Max(ReviewID) as MaxReviewID
                                FROM tblreview
                                WHERE Status <> 'Punch List Closed'
                                  AND Status <> ''
                                GROUP BY ReportId) as maxReview USING (ReportID)
                     LEFT JOIN tblreview b ON (b.ReviewID = maxReview.MaxReviewID)
                     INNER JOIN tblsiteinfo d ON (a.SiteInfoID = d.SiteInfoID)
                     INNER JOIN (SELECT Max(reportId) AS ReportId FROM tblreport GROUP BY SiteInfoID) c ON a.ReportId = c.ReportId
                    WHERE a.SiteInfoID > 0
                    AND a.PO like '%'
                    AND a.Project like '%'
                    AND a.Status = 'Pending HQ Review'
                    AND a.\`Regional Review Status\` = 'Regional Approved'
                    AND \`Submission Date\` like '2022%'
                    ORDER BY ReportId;
        `;
    const user = JSON.parse(await getUser());
    const results = await getSqlResults(mysql, sqlQuery);
    response.status(200).json({...results, user});
}

const getReportsBulkApproved = async (request, response) => {
    const [results, fields] = await mysql.query(
        `SELECT 
            a.SiteInfoID,
            a.ReportID,
            d.Region,
            d.\`PO\`,
            d.\`Project\`,
            a.\`Report Type\`,
            d.\`Technology\`,
            a.\`Stage\`,
            a.\`Report Name\`,
            d.\`Technology Base Acceptance Cluster\`,
            d.\`Site Name\`,
            d.\`LocationID\`,
            a.Submission,
            a.\`Submission Date\`,
            d.Owner,
            a.\`Prepared By\`,
            b.\`Reviewed By\`,
            b.\`Review Date\`,
            d.\`KPI COA Submitted\`,
            d.\`HOU KPI COA Approved\`,
            d.\`HOD KPI COA Approved\`,
            a.\`Report Path\`,
            a.\`Status\`,
            b.\`Comment\`,
            a.\`Submitted to Regional\`,
            d.\`CBOClusterName\`,
            d.\`SiteInfoID\`,
            a.\`Regional Review Done\`,
            a.\`Regional Review Status\`,
            a.\`Punch List Status\`,
            d.\`Main Activity\`,
            d.\`Process\`,
            \`Opti Allocation\`
        FROM tblreport as a
        INNER JOIN tblsiteinfo as d ON a.SiteInfoID = d.SiteInfoID    
        INNER JOIN (
        SELECT ReportID, Max(ReviewID) AS ReviewID FROM tblreview
                                         GROUP BY ReportId
        ) as lastReview USING (ReportID)
        INNER JOIN (
            SELECT SiteInfoID, Max(ReportID) AS ReportID FROM tblreport
            GROUP BY SiteInfoID
        ) as lastReport USING (ReportID)
         INNER JOIN tblreview as b USING (ReviewID)
        WHERE b.Comment = 'bulk approve'
        
        `);
    response.status(200).json({
            success: true,
            data: results,
            fields: fields.map(field => field.name)
        }
    );
}

async function reviewSingleReport(request, response, status, userName) {
    const {reportId} = request.params;
    if (!reportId) {
        response.status(400).json({
            success: false,
            message: "Missing reportId"
        });
        return;
    }
    await mysql.connect();

    try {
        await mysql.connection.beginTransaction();
        const [rows, fields] = await mysql.query(
            `
                UPDATE eproject_cm_sb.tblreport
                SET Status = ?
                WHERE ReportID = ?
            `,
            [status, reportId]
        );
        const reviewDate = new Date().toISOString().slice(0, 10);
        const [rows2, fields2] = await mysql.query(
            `
                INSERT INTO eproject_cm_sb.tblreview (\` ReportId \`, \` Reviewed By \`, \` Review Date \`, \` Comment
                                                      \`, \` Status \`)
                VALUES (?, ?, ?, ?, ?)
            `,
            [reportId, userName, reviewDate, 'bulk approve', status]
        );
        await mysql.connection.commit();
        response.status(200).json({
            success: true,
            message: "Report reviewed"
        });
    } catch (error) {
        await mysql.connection.rollback();
        logger.error(error);
        // pool.releaseConnection();
        throw error;
    }
}

async function revertReports(request, response, multipleReports = false) {

    if (!request.body.reportIds && !request.params.reportId) {
        response.status(400).json({
            success: false,
            message: "Missing reportId"
        });
        return;
    }
    const reportIds = multipleReports ? request.body.reportIds : [request.params.reportId];
    await mysql.connect();
    try {
        await mysql.connection.beginTransaction();
        const [rows, fields] = await mysql.query(
            `
                UPDATE eproject_cm_sb.tblreport
                SET Status = 'Pending HQ Review'
                WHERE ReportID IN (?)
            `,
            [reportIds]
        );
        const [rows2, fields2] = await mysql.query(
            `
                DELETE FROM eproject_cm_sb.tblreview
                WHERE ReportID IN (?)  
                AND Status = 'HQ Approved'
                AND comment = 'bulk approve'
            `,
            [reportIds]
        );
        await mysql.connection.commit();
        response.status(200).json({
            success: true,
            message: "Report status reverted"
        });
        return;
    } catch (error) {
        await mysql.connection.rollback();
        logger.error(error);
        throw error;
    }
}

async function reviewMultipleReports(request, response, status, userName) {
    const reportIds = request.body.reportIds;
    if (!reportIds) {
        response.status(400).json({
            success: false,
            message: "Missing reportId"
        });
        return;
    }
    await mysql.connect();
    try {
        await mysql.connection.beginTransaction();
        const [rows, fields] = await mysql.query(
            `
                UPDATE eproject_cm_sb.tblreport
                SET Status = ?
                WHERE ReportID IN (?)
            `,
            [status, reportIds]
        );
        const reviewDate = new Date().toISOString().slice(0, 10);
        const params = reportIds.map(reportId => [reportId, userName, reviewDate, 'bulk approve', status]);
        const [rows2, fields2] = await mysql.query(
            `
                INSERT INTO eproject_cm_sb.tblreview (\` ReportId \`, \` Reviewed By \`, \` Review Date \`, \` Comment
                                                      \`, \` Status \`)
                VALUES
                ?
            `,
            [params]
        );
        await mysql.connection.commit();
        response.status(200).json({
            success: true,
            message: "Report reviewed"
        });
    } catch (error) {
        await mysql.connection.rollback();
        logger.error(error);
        throw error;
    }

}

const reviewReport = (reviewType) => async (request, response) => {
    const action = request.body.action;
    const apiKey = request.headers['api'];
    const user = await getCacheKeyValue(`${apiKey}-celcom-user`);
    const userName = user.Name;
    logger.info(`${userName} is reviewing report`);
    if (!user || !userName) {
        response.status(401).json({
            success: false,
            message: "User not found!"
        });
        return;
    }
    let status;
    switch (action) {
        case "approve":
            status = "HQ Approved";
            reviewType === "single" && await reviewSingleReport(request, response, status, userName);
            reviewType === "multiple" && await reviewMultipleReports(request, response, status, userName)
            break;
        case "revert":
            reviewType === "single" && await revertReports(request, response, false);
            reviewType === "multiple" && await revertReports(request, response, true);
            if (!["single", "multiple"].includes(reviewType)) {
                response.status(400).json({
                    success: false,
                    message: "Invalid reviewType"
                });
                return;
            }
            break;
        default:
            response.status(400).json({
                    success: false,
                    message: "Invalid action"
                }
            );
    }
    if (!action) {
        response.status(400).json({
            success: false,
            message: "Missing action"
        });
        return;
    }


}
module.exports = {
    reviewReport,
    getReportsPendingHQReview,
    getReportsBulkApproved,
}