const PostgresBackend = require("../db/PostgresBackend");
const {getCookies} = require("./utils");
const pg = new PostgresBackend();

const getCellInfo = async (request, response) => {
    const {cellName} = request.query;
    console.log(getCookies(request));
    await pg.connect();
    pg.pool.query("SELECT * FROM dnb.public.tblcellid WHERE \"Cellname\" = $1", [cellName], (error, results) => {
        if (error) {
            throw error
        }
        response.status(200).json(results.rows)
    })
}
const getCurrentNominal = async (nominalId) => {
    const sqlQuery = "SELECT * FROM dnb.rfdb.rf_nominal WHERE WHERE nominal_id=$1";
    await pg.connect();
    pg.pool.query(sqlQuery, [nominalId], (error, results) => {
        if (error) {
            throw error;
        }
        return results.rows;
    });
}
const updateNominal = async (request, response) => {
    const {body} = request;
    const sqlQuery = "UPDATE dnb.rfdb.rf_nominal\n" +
        "SET\n" +
        "    nominal_siteid=$1,\n" +
        "    rf_pic=$2,\n" +
        "    active_model=$3,\n" +
        "    nominal_latitude=$4,\n" +
        "    nominal_longitude=$5,\n" +
        "    phase_deployment=$6,\n" +
        "    phase_commercial=$7\n" +
        "WHERE nominal_id=$8";
    const sqlParams = [
        body['nominal_siteid'],
        body['rf_pic'],
        body['active_model'],
        body['nominal_latitude'],
        body['nominal_longitude'],
        body['phase_deployment'],
        body['phase_commercial'],
        body['nominal_id'],
    ];
    await pg.connect();
    pg.pool.query(sqlQuery, sqlParams, (error, results) => {
        if (error) {
            throw error
        }
        response.status(200).json({result: 'success'});
    });
}

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
    await pg.connect();
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