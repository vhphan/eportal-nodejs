// postgres backend using postgres.js


const postgres = require('postgres');

const sql = postgres({
    host: 'localhost',
    port: 6543,
    database: 'dnb',
    username: process.env.PGDB_USER,
    password: process.env.PGDB_PASS,
})

module.exports = sql