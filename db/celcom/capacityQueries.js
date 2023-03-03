const {getSql} = require("#src/db/pgjs/PgJsBackend14");
const {roundJsonValues} = require("#src/db/utils");
const {logger} = require("#src/middleware/logger");
const operator = 'celcom';
const sql = getSql(operator);

const getWorstCellsList = async (req, res) => {
    const results = await sql`  
    with aging_table as (select "Sector ID", count(*) as aging
                     from celcom.capacity.cap_data
                              inner join celcom.capacity.last_eight_weeks lew
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
           coalesce(aging, 0)::int as aging,
           d_date.first_day_of_week
    from celcom.capacity.cap_data as t1
             inner join (select * from celcom.capacity.last_eight_weeks limit 1) latest_week on t1."Wk Num" = latest_week."Wk Num" and t1."Wk Year" = latest_week."Wk Year"
             left join aging_table on aging_table."Sector ID" = t1."Sector ID"
             inner join celcom.capacity.d_date on d_date."week_of_year" = t1."Wk Num" and d_date."year_actual" = t1."Wk Year"
    where "Final Status" in ('Required Upgrade', 'Optimize')
    `;
    res.status(200).json({
        success: true,
        data: roundJsonValues(results),
    });
};

function getFilters(req) {
    logger.info(req.query);
    const agings = req.query['agings']?.map(w => parseInt(w)) || [1, 2, 3, 4, 5, 6, 7, 8];
    const regions = req.query['regions'] || ['Eastern', 'Sabah', 'Sarawak'];
    const issueTags = req.query['issueTags'] || ['Untagged', 'Event', 'Alarm', 'Optimization', 'Congestion'];
    return {agings, regions, issueTags};
}

const getRegionalCountTrend = async (req, res) => {
    const {agings, regions, issueTags} = getFilters(req);
    const results = await sql`
        SELECT t1."Wk Year"::int,
               t1."Wk Num"::int,
               "Region",
               count(*)::int as count,
               d_date.first_day_of_week::date
        from celcom.capacity.cap_data as t1
                left join celcom.capacity.aging_table on aging_table."Sector ID" = t1."Sector ID"
                left join celcom.capacity.last_issue on last_issue.sector_id = t1."Sector ID"
                inner join celcom.capacity.issue_categories on issue_categories.id = last_issue.issue_id
                inner join celcom.capacity.d_date on d_date."week_of_year" = t1."Wk Num" and d_date."year_actual" = t1."Wk Year"
        where "Final Status" in ('Required Upgrade', 'Optimize')
            and aging in ${sql(agings)}
            and "Region" in ${sql(regions)}
            and issue_categories.issue_category in ${sql(issueTags)}           
        group by t1."Wk Year", t1."Wk Num", "Region", first_day_of_week
        order by t1."Wk Year", t1."Wk Num"
    `;
    res.status(200).json({
        success: true,
        data: roundJsonValues(results),
    });
};

const getMaxYearWeek = async (req, res) => {
    const results = await sql`
        SELECT max("year_week") as max_year_week
        from celcom.capacity.aging_per_sector_history
    `;
    res.status(200).json({
            success: true,
            data: roundJsonValues(results),
        }
    );
};

const getAvailableYearWeeks = async (req, res) => {
    const results = await sql`
        WITH t1 AS (
            SELECT DISTINCT "Wk Year" || '-' || to_char("Wk Num", 'fm00') as year_week
            FROM capacity.cap_data
        )
        SELECT t1.year_week
        FROM t1
        ORDER BY t1.year_week;
    `;
    res.status(200).json({
            success: true,
            data: roundJsonValues(results),
        }
    );
};

const getIssueCountTrend = async (req, res) => {
    const {agings, regions, issueTags} = getFilters(req);
    // language=PostgreSQL
    const results = await sql`
        SELECT t1."year_week",
               issue_categories.issue_category,
               sum(t1."aging") as count
        from celcom.capacity.aging_per_sector_history as t1
                 left join celcom.capacity.last_issue on last_issue.sector_id = t1."Sector ID"
                 inner join celcom.capacity.issue_categories on issue_categories.id = last_issue.issue_id
        where aging in ${sql(agings)}
          and "Region" in ${sql(regions)}
          and issue_categories.issue_category in ${sql(issueTags)}
        group by t1."year_week", issue_categories.issue_category
        order by t1."year_week", issue_categories.issue_category;
    `;
    res.status(200).json({
            success: true,
            data: roundJsonValues(results),
        }
    );
};

const getAgingCountTrend = async (req, res) => {
    const {agings, regions, issueTags} = getFilters(req);
    // language=PostgreSQL
    const results = await sql`
        With t1 as (select *,
                           CASE
                               WHEN aging = 1 THEN '1'
                               WHEN aging >= 2
                                   AND aging <= 4 THEN '2 to 4'
                               WHEN aging > 4 THEN '> 4'
                               END as aging_category
                    FROM celcom.capacity.aging_per_sector_history)
        SELECT t1."year_week",
               aging_category,
               count(*) as count
        from t1
                 left join celcom.capacity.last_issue on last_issue.sector_id = t1."Sector ID"
                 inner join celcom.capacity.issue_categories on issue_categories.id = last_issue.issue_id
        where aging in ${sql(agings)}
          and "Region" in ${sql(regions)}
          and issue_categories.issue_category in ${sql(issueTags)}
        group by t1."year_week", aging_category
        order by t1."year_week", aging_category;
    `;
    res.status(200).json({
            success: true,
            data: roundJsonValues(results),
        }
    );
};

const getOverallCountAndPercentageTrend = async (req, res) => {
    const {regions} = getFilters(req);
    // language=PostgreSQL
    const results = await sql`
        With t1 as (SELECT "Wk Year" || '-' || to_char("Wk Num", 'fm00') as year_week, Count(*) as count_total
                    FROM celcom.capacity.cap_data
                    GROUP BY "Wk Year", "Wk Num"),

             t2 as (SELECT year_week, Count(*) as count_congested
                    FROM celcom.capacity.aging_per_sector_history
                    where "Region" in ${sql(regions)}
                    group by year_week),

             time_frame as (select year_week, first_day_of_week
                            from capacity.d_week
                            where year_week >= (select min(year_week) from t2)
                              and year_week <= (select max(year_week) from t2))

        SELECT first_day_of_week,
               t1.year_week,
               t1.count_total::int,
               t2.count_congested::int,
               (round((100 * t2.count_congested::float / t1.count_total::float)::numeric, 2))::float as percentage
        from time_frame
                 left join t1 on t1.year_week = time_frame.year_week
                 left join t2 on t2.year_week = time_frame.year_week
    `;
    res.status(200).json({
            success: true,
            data: roundJsonValues(results),
        }
    );


};

module.exports = {
    getRegionalCountTrend,
    getWorstCellsList,
    getMaxYearWeek,
    getAvailableYearWeeks,
    getIssueCountTrend,
    getAgingCountTrend,
    getOverallCountAndPercentageTrend,

};