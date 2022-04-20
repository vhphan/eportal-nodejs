const sqlQueries = {

    cells: `
        SELECT json_build_object(
                       'type', 'Feature',
                       'geometry', ST_AsGeoJSON({{geomCol}})::json,
                       'properties', json_build_object(
                               'Cell Name', "Cellname",
                               'Site Name', "Sitename",
                               'SiteID', "SITEID",
                               'OnAir', status2 is not null
                           )
                   ) as f
        FROM dnb.public."{{tech}}_cells" as cells
                 LEFT JOIN dnb.rest.tbl_cell_info as cell_info
                           on cells."Cellname" = cell_info."cellname"
        WHERE "Region" = $1;
    `,


}

module.exports = sqlQueries;