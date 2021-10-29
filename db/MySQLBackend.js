const mySQLBackend = require("mysql2/promise");

class MySQLBackend {
    constructor(operator) {
        this.connection = null;
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
        return this.connection;
    }

    async disconnect() {
        return this.connection.end();
    }

    async query(sqlQuery) {
        await this.connect();
        const [rows, fields] = await this.connection.execute(
            sqlQuery,
        );
        return [rows, fields];
    }

    async numRows(sqlQuery) {
        let numRows;
        await this.connect();
        const [rows, fields] = await this.connection.execute(
            sqlQuery,
        );
        console.log(rows);
        return rows.length;
    }
}

module.exports = MySQLBackend;