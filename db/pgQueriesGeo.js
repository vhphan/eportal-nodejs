const PostgresBackend = require("../db/PostgresBackend");
const pg = new PostgresBackend();
const {
    cells
} = require('./dnbSqlQueriesGeo');

const getCells = function () {
    return async (request, response) => {
        const {system, size, region} = request.query;
        let geomCol;
        switch (size) {
            case 'n':
                geomCol = 'geom';
                break;
            case 'xs':
                geomCol = 'geometry_0.1';
                break;
            case 's':
                geomCol = 'geometry_0.2';
                break;
            case 'm':
            default:
                geomCol = 'geometry_0.5';
                break;

        }
        if (!['L7', 'N7', 'N3'].includes(system)) {
            return response.status(400).send({
                error: 'Invalid system'
            });
        }
        const sql = cells.replace('{{tech}}', system).replace('{{geomCol}}', geomCol);
        const pool = await pg.setupPool();
        const result = await pool.query(sql, [region]);
        const parsedRows = result.rows.map(row => row['f']);
        response.status(200).json({type: 'FeatureCollection', features: parsedRows});
    }
};

module.exports = {
    getCells
};