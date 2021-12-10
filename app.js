const express = require('express');
const app = express();
const port = 3001;
const general = require('./routes/general');
const celcom = require('./routes/celcom');
const dnb = require('./routes/dnb');
const dotenv = require('dotenv')
const cors = require('cors');
const PostgresBackend = require("./db/PostgresBackend");
const {createListener} = require("./db/utils");

const result = dotenv.config({path: './.env'})

if (result.error) {
    throw result.error
}
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
app.use('/node/public', general);
app.use('/node/celcom', celcom);
app.use('/node/dnb', dnb);


app.listen(port,
    () => console.log(`listening on ${port}`)
);

const pg = new PostgresBackend();
const client = pg.getClient();
createListener(client, 'db_change');