const sql = require('./PgJsBackend14');

const getRaster = async (request, response) => {
    const {raster} = request.params;
    const gridType = request.query['gridType'] || 'hex_100';
    const table = `dnb.rasters.nr_avg_${gridType}`;
    const results = await sql`
                SELECT json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::json,
                    'properties', json_build_object(
                    'value', "weighted_avg",
                    'count', "count"
                        )
                    ) as f
                FROM ${sql(table)}
                         WHERE folder=${raster};`
    response.status(200).json({
        success: true,
        data: {type: 'FeatureCollection', features: results.map(d => d['f'])},
        meta: {title: raster}
    });
}

const getRasterPointsInGrid = async (request, response) => {

    const {folder, lng, lat} = request.params;
    const gridType = request.query['gridType'] || 'hex_100';
    const table = `dnb.rasters.${gridType}`;
    const results = await sql`
        select 
            dnb.rasters.nr_raster.* 
        from dnb.rasters.nr_raster
                                inner join
        (select geom
        from ${sql(table)}
        where st_intersects(geom, st_setsrid(st_point(${lng}, ${lat}), '4326')))
        as hex
        on st_intersects(hex.geom, nr_raster.geometry)
        where folder=${folder};
    `;
    response.status(200).json({
        success: true,
        data: results
    });

}


module.exports = {
    getRaster,
    getRasterPointsInGrid
}