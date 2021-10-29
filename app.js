const express = require('express');
const auth = require("./auth");
const app = express();
const port = 3000;
const celcom = require('./routes/celcom');
const general = require('./routes/general');
const dnb = require('./routes/dnb');
const dotenv = require('dotenv')

const result = dotenv.config({path: './.env'})
console.log(result);

if (result.error) {
    throw result.error
}

console.log(result.parsed)
app.use('/node/public', general);
app.use('/node/celcom', celcom);
app.use('/node/dnb', dnb);

app.listen(port,
    () => console.log(`listening on ${port}`)
);