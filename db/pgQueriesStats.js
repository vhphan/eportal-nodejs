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
} = require("./dnbSqlQueries");
const {roundJsonValues} = require("./utils");

const getQueryAsJson = function (sql, params = []) {
    return async (request, response) => {
        const pool = await pg.setupPool();
        const result = await pool.query(sql, params);
        response.status(200).json(roundJsonValues(result.rows));
    };
}

const getCellQuery = function (sql) {
    return async (request, response) => {

        const {object} = request.query;
        const params = object ? [`${object}`] : ['%'];

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
const cellListNRQuery = getCellQuery(cellListNR);
const cellListLTEQuery = getCellQuery(cellListLTE);
const siteStatsQuery = getSiteStats();
const dailyPlmnCellQueryNR = getCellQuery(dailyPlmnCellNR)
const dailyPlmnCellQueryLTE = getCellQuery(dailyPlmnCellLTE)

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
    dailyPlmnCellQueryLTE
}
