const express = require('express');
const app = express();
const port = 3001;
const general = require('./routes/general');
const celcom = require('./routes/celcom');
const dnb = require('./routes/dnb');
const dotenv = require('dotenv')
const apiCache = require('apicache');
const cors = require('cors');
let cache = apiCache.middleware

const result = dotenv.config({path: './.env'})

if (result.error) {
    throw result.error
}
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
app.use(cache('5 minutes'));
app.use('/node/public', general);
app.use('/node/celcom', celcom);
app.use('/node/dnb', dnb);


app.listen(port,
    () => console.log(`listening on ${port}`)
);