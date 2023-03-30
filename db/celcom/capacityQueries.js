const {getSql} = require("#src/db/pgjs/PgJsBackend14");
const {roundJsonValues} = require("#src/db/utils");
const {logger} = require("#src/middleware/logger");
const operator = 'celcom';
const sql = getSql(operator);

const getWorstCellsList = async (req, res) => {
    // language=PostgreSQL
    const results = await sql`
        with latest_week as (select "Wk Year", "Wk Num"
                             from celcom.capacity.last_eight_weeks
                             order by "Wk Year" desc, "Wk Num" desc
                             limit 1)
        SELECT t1."Wk Year",
               t1."Wk Num",
               "LOCID",
               t1."Sector ID",
               "LAYER(S)",
               t1."Region",
               "Average PRB (Normalize)",
               "Average user TP 4BH (Mbps) Total",
               "Current Sector 4BH volume (GB)",
               "Final Status",
               coalesce(aging, 0)::int as aging,
               first_day_of_week
        from celcom.capacity.cap_data as t1
                 inner join celcom.capacity.aging_per_sector_history as t2 on t2."Sector ID" = t1."Sector ID"
            and t2.year_week = t1."Wk Year" || '-' || to_char("Wk Num", 'fm00')
                 inner join latest_week on latest_week."Wk Year" = t1."Wk Year" and latest_week."Wk Num" = t1."Wk Num"
                 inner join celcom.capacity.d_week
                            on d_week.year_week = t1."Wk Year" || '-' || to_char(t1."Wk Num", 'fm00')
        where "Final Status" in ('Required Upgrade')
          and aging >= 1
        --           and aging in ${sql(agings)}
        --           and t1."Region" in ${sql(regions)}
        ;
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
    return {agings, regions};
}

const getRegionalCountTrend = async (req, res) => {
    const {agings, regions} = getFilters(req);
    const results = await sql`
        SELECT year_week,
               "Region",
               count(*)::int as count,
       d_week.first_day_of_week::date
        from celcom.capacity.aging_per_sector_history as t1
            inner join celcom.capacity.d_week on t1.year_week = d_week.year_week
        where
            aging in ${sql(agings)}
            and "Region" in ${sql(regions)}
        group by "Region", first_day_of_week, year_week
        order by "Region", first_day_of_week, year_week;
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

const getAgingCountTrend = async (req, res) => {
    const {agings, regions} = getFilters(req);
    // language=PostgreSQL
    const results = await sql`

    SELECT year_week,
        "Region",
        d_week.first_day_of_week::date,
        count(*)::int                                   as count,
        sum(case when aging = 1 then 1 else 0 end)::int as aging_1,
        sum(case when aging = 2 then 1 else 0 end)::int as aging_2,
        sum(case when aging = 3 then 1 else 0 end)::int as aging_3,
        sum(case when aging = 4 then 1 else 0 end)::int as aging_4,
        sum(case when aging = 5 then 1 else 0 end)::int as aging_5,
        sum(case when aging = 6 then 1 else 0 end)::int as aging_6,
        sum(case when aging = 7 then 1 else 0 end)::int as aging_7,
        sum(case when aging = 8 then 1 else 0 end)::int as aging_8
    from celcom.capacity.aging_per_sector_history as t1
    inner join celcom.capacity.d_week on t1.year_week = d_week.year_week
        where aging in ${sql(agings)}
    and "Region" in ${sql(regions)}
        group by rollup ("Region"), first_day_of_week, year_week
        order by "Region", first_day_of_week, year_week;

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
    getAgingCountTrend,
};