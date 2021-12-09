const {Client, Pool} = require('pg');

class PostgresBackend {
    constructor() {
        this.pool = null;
        this.config = {};
    }

    setupPool() {
        if (!this.pool) {
            this.config = {
                user: process.env.PGDB_USER,
                password: process.env.PGDB_PASS,
                host: 'localhost',
                database: 'dnb',
                port: 6543,
            };
            this.pool = new Pool(this.config)
        }
        return this.pool;
    }

    getClient() {
        return new Client(this.config);
    }

    query(query, queryParams, callback) {
        const pool = this.setupPool();
        // const client = await pool.connect()
        // client.query(query, queryParams, (err, results) => {
        //     if (err) {
        //         console.error("ERROR: ", err)
        //     }
        //     if (err) {
        //         return callback(err)
        //     }
        //     callback(null, results.rows)
        // })
        pool.connect((err, client, done) => {
            if (err) return callback(err);
            client.query(query, queryParams, (err, results) => {
                done()
                if (err) {
                    console.error("ERROR: ", err)
                    return callback(err)
                }
                callback(null, results.rows)
            })
        });
    }
}

module.exports = PostgresBackend;