const PostgresBackend = require("../db/PostgresBackend");
const pg = new PostgresBackend();
const sql = require('../db/pgjs/PgJsBackend');

const {
    cells
} = require('./dnbSqlQueriesGeo');
const {logger} = require("../middleware/logger");

const getCells = async (request, response) => {
    const {system, size, region} = request.query;
    let geomCol;
    switch (size) {
        case 'n':
        case 'l':
            geomCol = 'cells.geom';
            break;
        case 'xs':
            geomCol = 'cells.geometry_0_1';
            break;
        case 's':
            geomCol = 'cells.geometry_0_2';
            break;
        case 'm':
        default:
            geomCol = 'cells.geometry_0_5';
            break;
    }
    if (!['L7', 'N7', 'N3'].includes(system)) {
        return response.status(400).send({
            error: 'Invalid system'
        });
    }
    // const sqlQuery = cells.replace('{{tech}}', system).replace('{{geomCol}}', geomCol).replace('{{region}}', region);
    // const pool = await pg.setupPool();
    // const result = await pool.query(sql, [region]);
    // const parsedRows = result.rows.map(row => row['f']);
    // logger.info(sqlQuery);
    const tableName = 'dnb.rfdb.' + system.toLowerCase() + '_cells'
    const cellInfoTableName = system.startsWith('N') ? 'dnb.rfdb.tbl_5g_cell_info' : 'dnb.rfdb.tbl_4g_cell_info'
    const results = await sql`SELECT json_build_object(
                       'type', 'Feature',
                       'geometry', ST_AsGeoJSON(${sql(geomCol)})::json,
                       'properties', json_build_object(
                               'Cell Name', "Cellname",
                               'Site Name', "Sitename",
                               'SiteID', "SITEID",
                               'OnAir', status2 is not null
                           )
                   ) as f
        FROM ${sql(tableName)} as cells
                 INNER JOIN ${sql(cellInfoTableName)} as cell_info
                           on cells."Cellname" = cell_info."cellname"
        WHERE "Region" = ${region}
        AND "LATITUDE" IS NOT NULL
        AND "LONGITUDE" IS NOT NULL
        AND "LATITUDE" > 0
        AND "LONGITUDE" > 0 `;
    console.log(results.length);
    response.status(200).json({type: 'FeatureCollection', features: results.map(d=>d['f'])});
}

module.exports = {
    getCells
};