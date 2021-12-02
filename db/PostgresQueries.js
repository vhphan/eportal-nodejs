const PostgresBackend = require("../db/PostgresBackend");

const pg = new PostgresBackend();

const getCellInfo = async (request, response) => {
    const {cellName} = request.query;
    console.log(cellName);
    await pg.connect();
    //"SELECT * FROM dnb.public.tblcellid WHERE \"Cellname\"='$cell_name' LIMIT 1; "
    pg.pool.query("SELECT * FROM dnb.public.tblcellid WHERE \"Cellname\" = $1", [cellName], (error, results) => {
        if (error) {
            throw error
        }
        response.status(200).json(results.rows)
    })
}
module.exports = {
    getCellInfo,
}