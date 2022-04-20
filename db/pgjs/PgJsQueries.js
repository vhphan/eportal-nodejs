const sql = require('./PgJsBackend');

const testQuery = async (request, response) => {
    const results = await sql`SELECT *
                   FROM dnb.public.sites_on_air
                   ORDER BY random()
                   LIMIT 10`;
    response.status(200).json(results);
}

module.exports = {
    testQuery
};