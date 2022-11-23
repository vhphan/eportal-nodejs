const postgres = require('postgres');

const sql = postgres({
    host: 'localhost',
    port: 5433,
    database: 'dnb',
    username: process.env.PGDB14_USER_DNB,
    password: process.env.PGDB14_PASS_DNB,
})

module.exports = sql