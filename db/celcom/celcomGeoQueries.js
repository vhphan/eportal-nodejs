const sql = require('./PgJsBackend');
const fs = require("fs");

const getCells = async (request, response) => {
    const {system, size} = request.query;
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
    if (!['L9', 'L18', 'L21', 'L26', 'G9', 'G18'].includes(system)) {
        return response.status(400).send({
            error: 'Invalid system'
        });
    }

    let results;

    results = await sql`SELECT json_build_object(
                       'type', 'Feature',
                       'geometry', ST_AsGeoJSON(${sql(geomCol)})::json,
                       'properties', json_build_object(
                               'Cell Name', "CELLname",
                               'Site Name', "Sitename",
                               'LocationID', "LOCID",
                               'OnAir', TRUE
                           )
                   ) as f
        FROM celcom.stats.all_cells as cells
        WHERE "Region" like ${region}
        AND "SystemID" = ${system}
        AND "LATITUDE" IS NOT NULL
        AND "LONGITUDE" IS NOT NULL
        AND "BSC_RNC_ERBS" IS NOT NULL  
        AND "LATITUDE" > 0
        AND "LONGITUDE" > 0 `;

    response.status(200).json({type: 'FeatureCollection', features: results.map(d => d['f'])});
}

const getClusters = async (request, response) => {
    fs.readFile('files/celcom/celcomClusters.geojson', 'utf8', (err, data) => {
        if (err) {
            throw err;
        }
        response.send(JSON.parse(data));
    });
}

module.exports = {
    getCells,
    getClusters,
}