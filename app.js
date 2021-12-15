const express = require('express');
const app = express();
const port = 3001;
const general = require('./routes/general');
const celcom = require('./routes/celcom');
const dnb = require('./routes/dnb');
const dotenv = require('dotenv')
const cors = require('cors');
const PostgresBackend = require("./db/PostgresBackend");
const format = require('pg-format');
const {isObject} = require("./db/utils");

const {createListener} = require("./db/utils");

const result = dotenv.config({path: './.env'})

if (result.error) {
    throw result.error
}
app.use(express.json());
app.use(express.urlencoded({extended: true}));
app.use(cors());
app.use('/node/public', general);
app.use('/node/celcom', celcom);
app.use('/node/dnb', dnb);


app.listen(port,
    () => console.log(`listening on ${port}`)
);

function createListeners() {
    const pg = new PostgresBackend();
    const client = pg.getClient();
    createListener(client, 'db_change', async (data) => {
        const payload = JSON.parse(data.payload);
        const newVal = payload['new_val'];
        const oldVal = payload['old_val'];
        let parseResults = [];

        Object.entries(newVal).forEach(([key, val]) => {
            if (val !== oldVal[key] && !isObject(val)) {
                // parseResults.push({
                //     table_name: payload['tabname'],
                //     column_name: key,
                //     old_value: oldVal[key],
                //     new_value: val,
                //     updated_by: newVal['last_user'],
                //     time_stamp: payload['tstamp']
                // })
                parseResults.push([
                    payload['tabname'],
                    key,
                    oldVal[key],
                    val,
                    newVal['last_user'],
                    payload['tstamp']
                ])
            }
        })
        console.log(parseResults);
        client.query(format('INSERT INTO logging.t_history_parsed (table_name, column_name, old_value, new_value, updated_by, time_stamp) VALUES %L', parseResults), [], (err, result) => {
            console.log(err);
            console.log(result);
        });
    });
}

createListeners();