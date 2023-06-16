const postgres = require('postgres');
require('dotenv').config();

const sqlElephant = postgres({
    host: process.env.ELEPHANT_HOST,
    port: 5432,
    database: process.env.ELEPHANT_DB,
    username: process.env.ELEPHANT_USER,
    password: process.env.ELEPHANT_PASSWORD,
});

module.exports = {
    sqlElephant,
    listenerElephant: async () => {
        console.log('listening to elephant');
        await sqlElephant.listen('events', payload => {
            const json = JSON.parse(payload);
            console.log(payload);
            console.log(json);
        });
    }
}