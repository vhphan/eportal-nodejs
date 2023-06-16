const MySQLBackend = require("../db/MySQLBackend");
const {logger} = require("../middleware/logger");
const {json2csvAsync} = require("json-2-csv");
const fs = require("fs");

const mysqlCelcom = new MySQLBackend('celcom');
const mysqlDnb = new MySQLBackend('dnb');
const mysqlDbMap = {
    'celcom': mysqlCelcom,
    'dnb': mysqlDnb
}

const downloadFoldersMap = {
    'celcom': '/home/eproject/celcom_files/downloads',
    'dnb': '/home/eproject/dnb/dnb_file_repo/downloads',
    'dev': "C:\\Temp"
}

async function downloadQueryAsFile(response, sql, operator, filterValues) {
    const db = mysqlDbMap[operator];
    await db.connect();
    const pool = db.pool;
    const result = await pool.query(sql, [...filterValues]);
    const folderToUse = process.env.NODE_ENV === 'development'? downloadFoldersMap['dev']: downloadFoldersMap[operator];
    json2csvAsync(result[0])
    .then((csv) => {
        const downloadFileName = `download_${Date.now()}.csv`;
        const fileName = `${folderToUse}/${downloadFileName}`;
        fs.writeFile(fileName, csv, (err) => {
            if (err) {
                logger.error(err);
                throw err;
            }
            response.download(fileName, downloadFileName);
        });
    })
}

const getTabulatorDataMySql = (operator) => async (request, response, next) => {
    const {schema, boolOperand, table, filters, sorters, download} = request.query;
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

    let sql = `
                SELECT *
                FROM ${schema}.${table}
                ${filterString}
                ${sorterString}
                `;
    if (download) {
        await downloadQueryAsFile(response, sql, operator, filterValues);
        return;
    }
    sql += `LIMIT ? OFFSET ?`;
    logger.info(sql);
    logger.info([...filterValues, size, offset])
    const sqlCount = `SELECT Count(*) as count FROM ${schema}.${table} ${filterString};`;
    logger.info(sqlCount);
    try {
        const db = mysqlDbMap[operator];
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