const PostgresBackend = require("../db/PostgresBackend");
const pg = new PostgresBackend();
const {
    dailyNetworkNR,
    dailyPlmnNR,
    dailyNetworkLTE,
    dailyPlmnLTE,
    siteList,
    dailySiteNR,
    dailySiteLTE
} = require("./dnbSqlQueries");
const {roundJsonValues} = require("./utils");

const getQueryAsJson = function (sql) {
    return async (request, response) => {
        const pool = await pg.setupPool();
        const result = await pool.query(sql, []);
        response.status(200).json(roundJsonValues(result.rows));
    };
}

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
const siteStatsQuery = getSiteStats();


module.exports = {
    dailyNetworkQueryNR,
    dailyPlmnQueryNR,
    dailyNetworkQueryLTE,
    dailyPlmnQueryLTE,
    siteListQuery,
    siteStatsQuery
}
