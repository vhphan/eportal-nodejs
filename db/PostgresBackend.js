const {Client} = require('pg');

class PostgresBackend {
    constructor() {
        this.pool = null;
    }

    async connect() {
        const user = process.env.PGDB_USER;
        const password = process.env.PGDB_PASS;
        const Pool = require('pg').Pool
        this.pool = new Pool({
            user,
            password,
            host: 'localhost',
            database: 'dnb',
            port: 6543,
        })
        return this.pool;
    }

}

module.exports = PostgresBackend;