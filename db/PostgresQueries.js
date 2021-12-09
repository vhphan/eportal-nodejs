const PostgresBackend = require("../db/PostgresBackend");
const {getCookies} = require("./utils");
const asyncHandler = require("../middleware/async");
const pg = new PostgresBackend();

const getCellInfo = async (request, response) => {
    const {cellName} = request.query;
    console.log(getCookies(request));
    await pg.setupPool();
    pg.pool.query("SELECT * FROM dnb.public.tblcellid WHERE \"Cellname\" = $1", [cellName], (error, results) => {
        if (error) {
            throw error
        }
        response.status(200).json(results.rows)
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
    console.log(rowBeforeUpdate, rowAfterUpdate);
}

const updateNominal = asyncHandler(async (request, response) => {
    const {body} = request;
    const sqlQuery = `UPDATE dnb.rfdb.rf_nominal
                      SET nominal_siteid=$1,
                          rf_pic=$2,
                          active_model=$3,
                          nominal_latitude=$4,
                          nominal_longitude=$5,
                          phase_deployment=$6,
                          phase_commercial=$7
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
    ];
    pg.query(sqlQuery, sqlParams, (error, results) => {
        if (error) throw error;
        console.log(results);
        response.status(200).json({result: 'success'});
    });
})

const updateConfigs = async (request, response) => {
    const {body} = request;
    const sqlQuery = "UPDATE rfdb.\"myesite_SectorLevelData\"\n" +
        "SET \"AntDirection\"=$1,\n" +
        "    \"AntHeight\"=$2,\n" +
        "    \"AntModel\"    =$3,\n" +
        "    \"AntEtilt\"    =$4,\n" +
        "    \"AntMtilt\"    =$5\n" +
        "WHERE \"LocationSiteId\" = $6\n" +
        "  AND \"SectorId\" = $7\n" +
        "  AND \"System\" = $8;";
    const sqlParams = [
        body['AntDirection'],
        body['AntHeight'],
        body['AntModel'],
        body['AntEtilt'],
        body['AntMtilt'],
        body['LocationSiteId'],
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
}


module.exports = {
    getCellInfo,
    updateNominal,
    updateConfigs,
}