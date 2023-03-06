const PostgresBackend = require("../db/PostgresBackend");
const {getCookies} = require("./utils");
const asyncHandler = require("../middleware/async");
const {isObject} = require("./utils");
const {use} = require("express/lib/router");
const {response} = require("express");
const {logger} = require("../middleware/logger");
const {saveToCache} = require("./RedisBackend");
const {getSql} = require("#src/db/pgjs/PgJsBackend14");
const {roundJsonValues} = require("#src/db/utils");
const dnbPg = new PostgresBackend('dnb');
const celcomPg = new PostgresBackend('celcom');

const testQuery = async () => {
    const pool = await dnbPg.setupPool();
    const result = await pool.query("SELECT * FROM dnb.public.sites_on_air ORDER BY random() LIMIT 10", []);
    return result.rows;
};

const getCellInfo = async (request, response) => {
    const {cellName} = request.query;
    await dnbPg.setupPool();
    dnbPg.pool.query("SELECT * FROM dnb.public.tblcellid WHERE \"Cellname\" = $1", [cellName], (error, results) => {
        if (error) {
            throw error;
        }
        response.status(200).json(results.rows);
    });
};

const getCurrentNominal = async (dnbIndex) => {
    const sqlQuery = "SELECT * FROM dnb.rfdb.rf_nominal WHERE dnb_index=$1";
    const pool = await dnbPg.setupPool();
    const result = pool.query(sqlQuery, [dnbIndex]);
    return result.rows;
};

const updateNominal = asyncHandler(async (request, response) => {
    const {body} = request;
    const userName = request.headers['username'] || 'unknown';
    console.log(userName);
    const sqlQuery = `UPDATE dnb.rfdb.rf_nominal
                      SET nominal_siteid=$1,
                          rf_pic=$2,
                          active_model=$3,
                          nominal_latitude=$4,
                          nominal_longitude=$5,
                          phase_deployment=$6,
                          phase_commercial=$7,
                          last_user=$9
                      WHERE dnb_index = $8`;
    const dnbIndex = body['dnb_index'];
    const sqlParams = [
        body['nominal_siteid'],
        body['rf_pic'],
        body['active_model'],
        body['nominal_latitude'],
        body['nominal_longitude'],
        body['phase_deployment'],
        body['phase_commercial'],
        dnbIndex,
        userName
    ];

    dnbPg.query(sqlQuery, sqlParams, (error, results) => {
        console.log('update executed');
        if (error) throw error;
        response.status(200).json({success: true});
    });

});

const updateConfigs = asyncHandler(async (request, response) => {
    const {body} = request;
    const userName = request.headers['username'] || 'unknown';

    const sqlQuery = "UPDATE rfdb.\"myesite_SectorLevelData\" as sector\n" +
        "SET \"AntDirection\"=$1,\n" +
        "    \"AntHeight\"=$2,\n" +
        "    \"AntModel\"    =$3,\n" +
        "    \"AntEtilt\"    =$4,\n" +
        "    \"AntMtilt\"    =$5,\n" +
        "    \"last_user\"    =$6\n" +
        "FROM rfdb.\"myesite_SiteLevelData\" as site\n" +
        "WHERE site.\"SiteName\" = sector.\"SiteName\"\n" +
        "  AND site.\"SiteProjectName\" = sector.\"SiteProjectName\"\n" +
        "  AND site.\"WorkplanID\" = $7\n" +
        "AND \"SectorId\"=$8 " +
        "AND \"System\"=$9;";
    const sqlParams = [
        body['AntDirection'],
        body['AntHeight'],
        body['AntModel'],
        body['AntEtilt'],
        body['AntMtilt'],
        userName,
        body['Workplan ID'],
        body['SectorId'],
        body['System'],
    ];
    await dnbPg.setupPool();
    dnbPg.pool.query(sqlQuery, sqlParams, (error, results) => {
        if (error) {
            throw error;
        }
        response.status(200).json({success: true});
    });
});

const dbFullViewData = async (request, response) => {
    const {queryName} = request.query;
    let sqlQuery;
    switch (queryName) {
        case 'nominal':
            sqlQuery = "SELECT dnb_index,\n" +
                "       nominal_siteid,\n" +
                "       nominal_id,\n" +
                "       rf_pic,\n" +
                "       active_model,\n" +
                "       morphology,\n" +
                "       nominal_latitude,\n" +
                "       nominal_longitude,\n" +
                "       phase_deployment,\n" +
                "       phase_commercial,\n" +
                "       nominal_change_log,\n" +
                "       \"District\",\n" +
                "       \"State\"\n" +
                "FROM dnb.rfdb.nominal_view;";
            break;
        case 'config':
            sqlQuery = "SELECT * FROM dnb.rfdb.config_view;";
            break;
        case 'candidates':
            sqlQuery = "SELECT * FROM dnb.rfdb.candidates_view;";
            break;
        default:
            sqlQuery = "SELECT 'NOTHING SELECTED' as info;";
            break;
    }
    dnbPg.query(sqlQuery, [], (error, results) => {
        if (error) throw error;
        response.status(200).json(results);
    });
};

const getChangeLog = asyncHandler(async (request, response) => {
    const tableName = request.query.tableName || 'all';
    let sqlQuery;
    if (tableName === 'all') {
        sqlQuery = "SELECT * FROM logging.t_history_parsed";
    } else {
        sqlQuery = "SELECT * FROM logging.t_history_parsed WHERE table_name=$1";
    }

    dnbPg.query(sqlQuery, tableName === 'all' ? [] : [tableName], (error, results) => {
        console.log(results);
        response.status(200).json(results);
    });

});

const addJob = async (request, response) => {
    const {body} = request;
    const jobInfo = body['jobInfo'] || null;
    if (jobInfo === null) {
        response.status(500).json({
            result: 'failure',
            message: 'job info not received.',
        });
        return;
    }
    //INSERT INTO TABLE_NAME (column1, column2, column3,...columnN)
    // VALUES (value1, value2, value3,...valueN);
    const sqlQuery = "INSERT INTO public.jobs (" +
        "job_info," +
        "status" +
        ") VALUES (" +
        "$1, $2);";
    const sqlParams = [
        jobInfo,
        'pending'
    ];
    await dnbPg.setupPool();
    dnbPg.pool.query(sqlQuery, sqlParams, (error, results) => {
        if (error) {
            throw error;
        }
        response.status(200).json({success: true});
    });
};

const getTabulatorConfig = async (request, response) => {
    const {userId} = request.query;
    const {key} = request.query;
    console.log(userId);
    const sqlQuery = "SELECT * FROM dnb.rfdb.tabulator_config WHERE user_id=$1 and key like $2";
    const sqlParams = [
        userId, key
    ];
    dnbPg.query(sqlQuery, sqlParams, (error, results) => {
        if (error) throw error;
        response.status(200).json(results);
    });
};

const saveTabulatorConfig = async (request, response) => {
    const {body} = request;
    const userId = body['userId'];
    const key = body['key'];
    const value = body['value'];
    const sqlQuery = "INSERT INTO dnb.rfdb.tabulator_config (user_id, key, value) VALUES (" +
        "$1, $2, $3);";
    const sqlParams = [
        userId, key, value
    ];
    await dnbPg.setupPool();
    dnbPg.pool.query(sqlQuery, sqlParams, (error, results) => {
        if (error) {
            throw error;
        }
        response.status(200).json({success: true});
    });
};

const getGeoJSON = async (request, response) => {
    const {system, region, size, onAir} = request.query;
    const sql = "SELECT json_build_object(\n" +
        "               'type', 'Feature',\n" +
        "               'geometry', ST_AsGeoJSON(geom)::json,\n" +
        "               'properties', json_build_object(\n" +
        "                       'Cell Name', \"Cellname\",\n" +
        "                       'SiteID', \"siteid\"\n" +
        "                   )\n" +
        "           ) as f\n" +
        "FROM dnb.public.\"N7_cells\"\n" +
        "WHERE \"Region\" = 'CENTRAL'";
};

const getTabulatorData = (operator = 'dnb', pgVersion = 12) => async (request, response, next) => {
    const {page, size, schema, table} = request.query;
    let {filters, filter, sorter, sorters, sort, sorts, boolOperand} = request.query;
    const offset = size * (page - 1);
    let filterArray = [];
    let filterValues = [];
    let i = 1;
    filters = filters || filter;
    sorters = sorters || sorter;
    sorters ??= sort || sorts;
    boolOperand = boolOperand || 'and';

    if (filters) {
        const filterTypes = {
            like: 'like',
            in: 'in',
        };

        filters.forEach(f => {

            if (typeof f['value'] === 'object') {
                if ('start' in f['value']) {
                    let startValue = f['value']['start'];
                    if (startValue === '') {
                        return;
                    }
                    filterArray.push(`${table}."${f['field']}" >= $${i++}`);
                    filterValues.push(Number(startValue));
                }
                if ('end' in f['value']) {
                    let endValue = f['value']['end'];
                    if (endValue === '') {
                        return;
                    }
                    filterArray.push(`${table}."${f['field']}" <= $${i++}`);
                    filterValues.push(Number(endValue));
                }
                if (f['type'] === filterTypes.in) {
                    filterArray.push(`${table}."${f['field']}" = ANY($${i++})`);
                    Array.isArray(f['value']) && filterValues.push([f['value']]);
                    !Array.isArray(f['value']) && filterValues.push(`${f['value']}`);
                    // filterValues.push(`${f['value']}`);
                }
                return;
              }

            switch (f['type']) {
                case filterTypes.like:
                    filterArray.push(`${table}."${f['field']}" ilike $${i++}`);
                    filterValues.push(`%${f['value']}%`);
                    break;
                case filterTypes.in:
                    filterArray.push(`${table}."${f['field']}" = ANY($${i++})`);
                    Array.isArray(f['value']) && filterValues.push([f['value']]);
                    !Array.isArray(f['value']) && filterValues.push(`${f['value']}`);
                    break;
            }

        });
    }
    const filterString = filterArray.length ? ' WHERE ' + filterArray.join(" " + boolOperand + " ") : '';


    let sorterArray = [];
    if (sorters) {
        sorters.forEach(sorter => {
            let field = sorter['field'];
            let dir = sorter['dir'];
            let quote = '"';
            sorterArray.push(`${quote}${field}${quote} ${dir}`);
        });
    }
    const sorterString = sorterArray.length ? ` Order By ${sorterArray.join(', ')}` : '';

    const sql = `SELECT *
                 FROM ${operator}.${schema}."${table}"
                   ${filterString}
                   ${sorterString}
                 LIMIT $${i++} OFFSET $${i++}`;

    const sqlCount = `SELECT Count(*) as count FROM ${operator}.${schema}."${table}" ${filterString};`;

    logger.info(sql);
    logger.info([...filterValues, size, offset]);

    try {

        let resultObj;
        let pg;

        if (pgVersion === 12) {
            pg = (operator === 'dnb' ? dnbPg : celcomPg);
        }
        if (pgVersion === 14) {
            pg = new PostgresBackend(operator, 14);
        }
        const pool = await pg.setupPool();
        const result = await pool.query(sql, [...filterValues, size, offset]);
        const countResult = await pool.query(sqlCount, filterValues);
        resultObj = {
            data: roundJsonValues(result.rows),
            count: parseInt(countResult.rows[0].count),
            last_page: Math.ceil(countResult.rows[0]['count'] / size),
            success: true
        };
        if (!countResult.rows[0]['count']) {
            logger.info(`no data for ${request.originalUrl}`);
        }
        return response.status(200).json(resultObj);
        // response.status(200).json({cache: false, ...resultObj});
        // saveToCache(request, resultObj).then(r => {logger.info(`saved to cache ${request.originalUrl}`)});

    } catch (e) {
        next(e);
    }

};

module.exports = {
    getCellInfo,
    updateNominal,
    updateConfigs,
    dbFullViewData,
    getChangeLog,
    addJob,
    getGeoJSON,
    saveTabulatorConfig,
    getTabulatorConfig,
    testQuery,
    getTabulatorData
};
