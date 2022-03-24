const {Client, Pool} = require('pg');
const {createSubscriber} = require("pg-listen");
const dotenv = require('dotenv');
const pg = require("pg");
const result = dotenv.config();

pg.types.setTypeParser(pg.types.builtins.FLOAT8, (value) => {
    return parseFloat(value);
});
pg.types.setTypeParser(pg.types.builtins.NUMERIC, (value) => {
    return parseFloat(value);
});
pg.types.setTypeParser(1114, function(stringValue) {
  return stringValue;  //1114 for time without timezone type
});
pg.types.setTypeParser(1082, function(stringValue) {
  return stringValue;  //1082 for date type
});

if (result.error) {
    throw result.error;
}

// console.log('dotenv parsed', result.parsed);


class PostgresBackend {
    constructor() {
        this.pool = null;
        this.client = null;
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
        this.config = {
            user: process.env.PGDB_USER,
            password: process.env.PGDB_PASS,
            host: 'localhost',
            database: 'dnb',
            port: 6543,
        };
        return new Client(this.config);
    }

    query(sql, queryParams, callback) {
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
            client.query(sql, queryParams, (err, results) => {
                done()
                if (err) {
                    console.error("ERROR: ", err)
                    return callback(err)
                }
                callback(null, results.rows)
            })
        });
    }

    getSubscriber() {
        // const databaseUrl = process.env.DB_URL_RFDB;
        const databaseUrl = '"postgres://postgres:postgres@localHost:6543/rfdb"';
        console.log('dburl', databaseUrl);
        return createSubscriber({connectionString: databaseUrl});
    }
}

module.exports = PostgresBackend;