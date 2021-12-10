const mySQLBackend = require("mysql2/promise");

class MySQLBackend {
    constructor(operator) {
        // this.connection = null;
        this.pool = null;
        this.operator = operator;
    }

    async connect() {
        let user, db, password;
        switch (this.operator) {
            case 'celcom':
                user = process.env.DB_USER_CM;
                password = process.env.DB_PASS_CM;
                db = 'eproject_cm';
                break;
            case 'dnb':
                user = process.env.DB_USER_DNB;
                password = process.env.DB_PASS_DNB;
                db = 'eproject_dnb';
                break;
        }
        this.connection = await mySQLBackend.createConnection({
            host: "localhost",
            port: 3306,
            user,
            password,
            database: db,
        });
        this.pool = await mySQLBackend.createPool({
            connectionLimit: 10,
            host: "localhost",
            port: 3306,
            user,
            password,
            database: db,
        });
        //   const pool  = mysql2.createPool({
        //   connectionLimit : 10,
        //   host            : 'example.org',
        //   user            : 'bob',
        //   password        : 'secret'
        // });
    }


    async query(sqlQuery, sqlParams) {
        await this.connect();
        // const [rows, fields] = await this.connection.execute(
        //     sqlQuery, sqlParams
        // );
        const [rows, fields] = await this.pool.query(
            sqlQuery, sqlParams
        );
        return [rows, fields];
    }

    async numRows(sqlQuery, sqlParams) {
        let numRows;
        await this.connect();
        // const [rows, fields] = await this.connection.execute(
        //     sqlQuery, sqlParams
        // );
        const [rows, fields] = await this.pool.query(
            sqlQuery, sqlParams
        );
        return rows.length;
    }
}

module.exports = MySQLBackend;