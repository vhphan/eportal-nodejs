// postgres backend using postgres.js


const postgres = require('postgres');

const sql = postgres({
    host: 'localhost',            // Postgres ip address[s] or domain name[s]
    port: 6543,          // Postgres server port[s]
    database: 'celcom',            // Name of database to connect to
    username: process.env.PGDB_USER,           // Username of database user
    password: process.env.PGDB_PASS,            // Password of database user
}) // will use psql environment variables

module.exports = sql