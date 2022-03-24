const PostgresBackend = require("../db/PostgresBackend");
const pg = new PostgresBackend();
const {
    dailyNetworkNR,
    dailyPlmnNR,
    dailyNetworkLTE,
    dailyPlmnLTE,
    siteList,
    dailySiteNR,
    dailySiteLTE,
    cellListLTE,
    cellListNR,
    dailyPlmnCellNR,
    dailyPlmnCellLTE,
    dailyNetworkCellNR,
    dailyNetworkCellLTE,

} = require("./dnbSqlQueries");

const {
    hourlyNetworkCellLTE,
    hourlyNetworkLTE,
    hourlyPlmnCellLTE,
    hourlyPlmnLTE,

    hourlyPlmnNR,
    hourlyNetworkNR,
    hourlyNetworkCellNR,
    hourlyPlmnCellNR,

} = require("./dnbSqlQueriesHourly")

const {roundJsonValues} = require("./utils");

const emptyPlaceHolder = async (req, res) => {
    res.status(200).json([]);
}


const getQueryAsJson = function (sql, params = []) {
    return async (request, response) => {
        const pool = await pg.setupPool();
        const result = await pool.query(sql, params);
        response.status(200).json(roundJsonValues(result.rows));
    };
}

const getCellQuery = function (sql, wildCard = false) {
    return async (request, response) => {

        const {object} = request.query;

        const params = object ? (wildCard ? [`%${object}%`] : [`${object}`]) : ['%'];

        const pool = await pg.setupPool();
        const result = await pool.query(sql, params);
        response.status(200).json(roundJsonValues(result.rows));
    };
};

const getSiteStats = function () {
    return async (request, response) => {
        const {site, tech} = request.query;
        let sql;
        if (tech === 'NR') {
            sql = dailySiteNR;
        }
        if (tech === 'LTE') {
            sql = dailySiteLTE;
        }
        const pool = await pg.setupPool();
        const result = await pool.query(sql, [site]);
        response.status(200).json(roundJsonValues(result.rows));
    };
}

const dailyNetworkQueryNR = getQueryAsJson(dailyNetworkNR);
const dailyNetworkQueryLTE = getQueryAsJson(dailyNetworkLTE);
const dailyPlmnQueryNR = getQueryAsJson(dailyPlmnNR);
const dailyPlmnQueryLTE = getQueryAsJson(dailyPlmnLTE);
const siteListQuery = getQueryAsJson(siteList);
const cellListNRQuery = getCellQuery(cellListNR, true);
const cellListLTEQuery = getCellQuery(cellListLTE, true);
const siteStatsQuery = getSiteStats();
const dailyPlmnCellQueryNR = getCellQuery(dailyPlmnCellNR)
const dailyPlmnCellQueryLTE = getCellQuery(dailyPlmnCellLTE)
const dailyNetworkCellQueryNR = getCellQuery(dailyNetworkCellNR)
const dailyNetworkCellQueryLTE = getCellQuery(dailyNetworkCellLTE)

const hourlyNetworkCellQueryLTE = getCellQuery(hourlyNetworkCellLTE)
const hourlyNetworkQueryLTE = getQueryAsJson(hourlyNetworkLTE)
const hourlyPlmnCellQueryLTE = getCellQuery(hourlyPlmnCellLTE)
const hourlyPlmnQueryLTE = getQueryAsJson(hourlyPlmnLTE)

const hourlyNetworkQueryNR = getQueryAsJson(hourlyNetworkNR);
const hourlyPlmnQueryNR = getQueryAsJson(hourlyPlmnNR);
const hourlyNetworkCellQueryNR = getCellQuery(hourlyNetworkCellNR);
const hourlyPlmnCellQueryNR = getCellQuery(hourlyPlmnCellNR);

module.exports = {
    dailyNetworkQueryNR,
    dailyPlmnQueryNR,
    dailyNetworkQueryLTE,
    dailyPlmnQueryLTE,
    siteListQuery,
    siteStatsQuery,
    cellListLTEQuery,
    cellListNRQuery,
    dailyPlmnCellQueryNR,
    dailyPlmnCellQueryLTE,
    dailyNetworkCellQueryNR,
    dailyNetworkCellQueryLTE,

    hourlyNetworkCellQueryLTE,
    hourlyNetworkQueryLTE,
    hourlyPlmnCellQueryLTE,
    hourlyPlmnQueryLTE,

    hourlyNetworkQueryNR,
    hourlyNetworkCellQueryNR,
    hourlyPlmnQueryNR,
    hourlyPlmnCellQueryNR,
}
