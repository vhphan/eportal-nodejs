const sql = require('./PgJsBackend14');

const getRaster = async (request, response) => {
    const {raster} = request.params;
    const results = await sql`
                SELECT json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::json,
                    'properties', json_build_object(
                    'value', "weighted_avg",
                    'count', "count"
                        )
                    ) as f
                FROM dnb.rasters.nr_avg_hex_100
                         WHERE folder=${raster};`
    response.status(200).json({
        success: true,
        data: {type: 'FeatureCollection', features: results.map(d => d['f'])},
        meta: {title: raster}
    });

}


module.exports = {
    getRaster
}