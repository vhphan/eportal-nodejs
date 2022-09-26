const MySQLBackend = require("../db/MySQLBackend");
const {logger} = require("../middleware/logger");

const mysqlCelcom = new MySQLBackend('celcom');
const mysqlDnb = new MySQLBackend('dnb');


const getTabulatorDataMySql = (operator) => async (request, response, next) => {
    const {schema, boolOperand, table, filters, sorters} = request.query;
    let {page, size} = request.query;
    [page, size] = [parseInt(page), parseInt(size)];
    const offset = size * (page - 1);

    let filterArray = [];
    let filterValues = [];
    if (filters) {
        filterArray = filters.map(f => {
            if (typeof f['value'] === "object" && f['type'] !== "in") {
                if ('start' in f['value']) {
                    return `${table}.\`${f['field']}\` >= ?`;
                }
                if ('end' in f['value']) {
                    return `${table}.\`${f['field']}\` <= ?`;
                }
                return;
            }
            if (f['type'] === "in") {
                return `${table}.\`${f['field']}\` IN (?)`;
            }
            return `${table}.\`${f['field']}\` ${f['type']} ?`;
        })


        filterValues = filters.map(f => {
            if (typeof f['value'] === "object" && f['type'] !== "in") {
                if ('start' in f['value']) {
                    return f['value']['start'];
                }
                if ('end' in f['value']) {
                    return f['value']['end'];
                }
                return;
            }
            if (f['type'] === "in") {
                return f['value'];
            }
            return `%${f['value']}%`;
        })
    }

    let sorterArray = [];
    if (sorters) {
        sorterArray = sorters.map(s => `\`${s['field']}\` ${s['dir']}`)
    }

    const sorterString = sorterArray.length ? ` Order By ${sorterArray.join(', ')}` : '';
    const filterString = filterArray.length ? ' WHERE ' + filterArray.join(" " + boolOperand + " ") : '';
    logger.info(`filterString: ${filterString}`);
    logger.info(`filterValues: ${filterValues}`);
    const sql = `SELECT *
                 FROM ${schema}.${table}
                   ${filterString}
                   ${sorterString}
                 LIMIT ? OFFSET ?`;

    logger.info(sql);
    logger.info([...filterValues, size, offset])
    const sqlCount = `SELECT Count(*) as count FROM ${schema}.${table} ${filterString};`;
    logger.info(sqlCount);
    try {

        const db = operator === 'dnb' ? mysqlDnb : mysqlCelcom;
        await db.connect();
        const pool = db.pool;
        const result = await pool.query(sql, [...filterValues, size, offset]);
        const countResult = await pool.query(sqlCount, filterValues);
        let resultObj = {
            data: result[0],
            count: parseInt(countResult[0][0].count),
            last_page: Math.ceil(countResult[0][0]['count'] / size)
        };
        response.status(200).json(resultObj);
    } catch (e) {
        next(e);
    }

}

module.exports = {
    getTabulatorDataMySql
}