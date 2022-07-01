const PostgresBackend = require("../db/PostgresBackend");
const pg = new PostgresBackend();
const sql = require('../db/pgjs/PgJsBackend');

const {
    cells
} = require('./dnbSqlQueriesGeo');
const {logger} = require("../middleware/logger");
const fs = require("fs");

const getCells = async (request, response) => {
    const {system, size, stats} = request.query;
    let {region} = request.query;
    region = region === 'all' ? '%' : region;
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

    let results;
    if (stats) {
        results = await sql`SELECT json_build_object(
                       'type', 'Feature',
                       'geometry', ST_AsGeoJSON(${sql(geomCol)})::json,
                       'properties', json_build_object(
                               'Cell Name', "Cellname",
                               'Site Name', "Sitename",
                               'SiteID', "SITEID",
                               'OnAir', TRUE

                           )
                   ) as f
        FROM ${sql(tableName)} as cells
                 INNER JOIN dnb.stats.cells_in_stats as cell_info
                   on cells."Cellname" = cell_info."object"
        WHERE "Region" like ${region}
        AND "LATITUDE" IS NOT NULL
        AND "LONGITUDE" IS NOT NULL
        AND "LATITUDE" > 0
        AND "LONGITUDE" > 0 `;
    } else {
        results = await sql`SELECT json_build_object(
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
        WHERE "Region" like ${region}
        AND "LATITUDE" IS NOT NULL
        AND "LONGITUDE" IS NOT NULL
        AND "LATITUDE" > 0
        AND "LONGITUDE" > 0 `;
    }
    response.status(200).json({type: 'FeatureCollection', features: results.map(d => d['f'])});
}

const getClusters = async (request, response) => {
    fs.readFile('files/clusters.geojson', 'utf8', (err, data) => {
        if (err) {
            throw err;
        }
        response.send(JSON.parse(data));
    });
}

module.exports = {
    getCells,
    getClusters
};