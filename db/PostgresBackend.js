const {Client, Pool} = require('pg');

class PostgresBackend {
    constructor() {
        const user = process.env.PGDB_USER;
        const password = process.env.PGDB_PASS;
        this.pool = null;
        this.config = {
            user,
            password,
            host: 'localhost',
            database: 'dnb',
            port: 6543,
        };
    }

    config;

    async connect() {
        this.pool = new Pool(this.config)
        return this.pool;
    }

    getClient() {
        return new Client(this.config);
    }
}

module.exports = PostgresBackend;