const {getSql} = require("#src/db/pgjs/PgJsBackend14");
const {roundJsonValues} = require("#src/db/utils");
const operator = 'celcom';
const sql = getSql(operator);

const getWorstCellsList = async (req, res) => {
    const results = await sql`  
    with aging_table as (select "Sector ID", count(*) as aging
                     from capacity.cap_data
                              inner join capacity.last_eight_weeks lew
                                         on cap_data."Wk Num" = lew."Wk Num" and cap_data."Wk Year" = lew."Wk Year"
                     where "Final Status" in ('Required Upgrade', 'Optimize')
                     group by "Sector ID")
    SELECT t1."Wk Year",
           t1."Wk Num",
           "LOCID",
           t1."Sector ID",
           "LAYER(S)",
           "Region",
           "Average PRB (Normalize)",
           "Average user TP 4BH (Mbps) Total",
           "Current Sector 4BH volume (GB)",
           "Final Status",
           coalesce(aging, 0) as aging
    from capacity.cap_data as t1
             inner join (select * from capacity.last_eight_weeks limit 1) latest_week on t1."Wk Num" = latest_week."Wk Num" and t1."Wk Year" = latest_week."Wk Year"
             left join aging_table on aging_table."Sector ID" = t1."Sector ID"
    where "Final Status" in ('Required Upgrade', 'Optimize')
    `;
    res.status(200).json({
        success: true,
        data: roundJsonValues(results),
    });
};

const getRegionalCountTrend = async (req, res) => {
    const results = await sql`
        SELECT t1."Wk Year",
               t1."Wk Num",
               "Region",
        --        coalesce(aging, 0) as aging,
               count(*) as count
        from celcom.capacity.cap_data as t1
        --          inner join (select * from capacity.last_eight_weeks limit 1) latest_week
        --                     on t1."Wk Num" = latest_week."Wk Num" and t1."Wk Year" = latest_week."Wk Year"
                 left join celcom.capacity.aging_table on aging_table."Sector ID" = t1."Sector ID"
                 left join celcom.capacity.last_issue on last_issue.sector_id = t1."Sector ID"
        where "Final Status" in ('Required Upgrade', 'Optimize')
          and "Region" in ('Eastern', 'Sabah', 'Sarawak')
          and last_issue.issue_id in (1, 2, 3, 4, 5)
        group by t1."Wk Year", t1."Wk Num", "Region";
    `;
    res.status(200).json({
        success: true,
        data: roundJsonValues(results),
    });
};


module.exports = {
    getRegionalCountTrend,
    getWorstCellsList,
};