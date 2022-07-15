const MySQLBackend = require("../../db/MySQLBackend");

// const mysql = new MySQLBackend('celcom');
const mysqlTest = new MySQLBackend('celcom_test');

const getReportsPendingHQReview = async (request, response) => {

    const [rows, fields] = await mysqlTest.query(
        `
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
            FROM tblreport a
             LEFT JOIN (SELECT ReportID, Max(ReviewID) as MaxReviewID
                        FROM tblreview
                        WHERE Status <> 'Punch List Closed'
                          AND Status <> ''
                        GROUP BY ReportId) as maxReview USING (ReportID)
             LEFT JOIN tblreview b ON (b.ReviewID = maxReview.MaxReviewID)
             INNER JOIN tblsiteinfo d ON (a.SiteInfoID = d.SiteInfoID)
             INNER JOIN (SELECT Max(reportId) AS ReportId FROM tblreport GROUP BY SiteInfoID) c ON a.ReportId = c.ReportId
            WHERE a.SiteInfoID > 0
            AND 1
            AND a.PO like '%'
            AND a.Project like '%'
            AND a.Status  = 'Pending HQ Review'
            AND a.\`Regional Review Status\` = 'Regional Approved'
            AND \`Submission Date\` like '2022%'
            ORDER BY ReportID;
        `
    );
    response.json({
        success: true,
        data: rows,
        fields: fields.map(field => field.name)
    });

}

module.exports = {
    getReportsPendingHQReview,
}