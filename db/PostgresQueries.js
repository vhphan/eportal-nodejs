const PostgresBackend = require("../db/PostgresBackend");
const {getCookies} = require("./utils");
const asyncHandler = require("../middleware/async");
const {isObject} = require("./utils");
const pg = new PostgresBackend();

const getCellInfo = async (request, response) => {
    const {cellName} = request.query;
    await pg.setupPool();
    pg.pool.query("SELECT * FROM dnb.public.tblcellid WHERE \"Cellname\" = $1", [cellName], (error, results) => {
        if (error) {
            throw error
        }
        response.status(200).json(results.rows);
    })
}

const getCurrentNominal = async (dnbIndex) => {
    const sqlQuery = "SELECT * FROM dnb.rfdb.rf_nominal WHERE dnb_index=$1";
    const pool = await pg.setupPool();
    const result = pool.query(sqlQuery, [dnbIndex]);
    return result.rows;
};

async function logChanges(dnbIndex, rowBeforeUpdate) {
    const rowAfterUpdate = await getCurrentNominal(dnbIndex);
}

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

    pg.query(sqlQuery, sqlParams, (error, results) => {
        console.log('update executed');
        if (error) throw error;
        response.status(200).json({result: 'success'});
    });

})

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
    await pg.setupPool();
    pg.pool.query(sqlQuery, sqlParams, (error, results) => {
        if (error) {
            throw error
        }
        response.status(200).json({result: 'success'});
    });
})

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
                "FROM dnb.rfdb.nominal_view;"
            break;
        case 'config':
            sqlQuery = "SELECT * FROM dnb.rfdb.config_view;"
            break;
        case 'candidates':
            sqlQuery = "SELECT * FROM dnb.rfdb.candidates_view;"
            break;
        default:
            sqlQuery = "SELECT 'NOTHING SELECTED' as info;"
            break;
    }
    pg.query(sqlQuery, [], (error, results) => {
        if (error) throw error;
        response.status(200).json(results);
    });
}

const getChangeLog = asyncHandler(async (request, response) => {
    const {tableName} = request.query || 'all';
    let sqlQuery;
    if (tableName === 'all') {
        sqlQuery = "SELECT * FROM logging.t_history";
    } else {
        sqlQuery = "SELECT * FROM logging.t_history WHERE tabname=$1";
    }

    pg.query(sqlQuery, [tableName], (error, results) => {
        console.log(results);
        let parseResults = [];
        results.forEach((result) => {
            const newVal = result['new_val'];
            const oldVal = result['old_val'];
            Object.entries(newVal).forEach(([key, val]) => {
                if (val !== oldVal[key] && !isObject(val)) {
                    parseResults.push({
                        table_name: result['tabname'],
                        column_name: key,
                        old_value: oldVal[key],
                        new_value: val,
                        updated_by: newVal['last_user'],
                        time_stamp: result['tstamp']
                    })
                }
            })
        })

        const finalResults = {}
        response.status(200).json(parseResults);
    });

});


module.exports = {
    getCellInfo,
    updateNominal,
    updateConfigs,
    dbFullViewData,
    getChangeLog
}