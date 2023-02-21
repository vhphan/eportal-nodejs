const postgres = require('postgres');

const sqlDnb = postgres({
    host: 'localhost',
    port: 5433,
    database: 'dnb',
    username: process.env.PGDB14_USER_DNB,
    password: process.env.PGDB14_PASS_DNB,
});

const sqlCelcom = postgres({
    host: 'localhost',
    port: 5433,
    database: 'celcom',
    username: process.env.PGDB14_USER_CELCOM,
    password: process.env.PGDB14_PASS_CELCOM,
});

getSql = (operator) => {
    switch (operator) {
        case 'dnb':
            return sqlDnb;

        case 'celcom':
            return sqlCelcom;
    }
};

module.exports = {
    sqlDnb,
    sqlCelcom,
    getSql

};