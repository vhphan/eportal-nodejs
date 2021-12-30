const express = require('express');
const socket = require('socket.io');
const app = express();
const port = 3001;
const general = require('./routes/general');
const celcom = require('./routes/celcom');
const dnb = require('./routes/dnb');
const dotenv = require('dotenv')
const cors = require('cors');
const PostgresBackend = require("./db/PostgresBackend");
const format = require('pg-format');
const {createListeners} = require("./db/utils");
const {logger} = require("./middleware/logger");
const {isObject} = require("./db/utils");
const errorHandler = require('./middleware/error');
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

app.use(errorHandler);


const server = app.listen(port,
    () => {
        console.log(`listening on ${port}`);
        logger.info(
            'test'
        );
    }
);

const socketServer = socket(
    server,
    {
        cors: {
            origin: '*',
        },
        path: '/node/socket.io'
    }
);

const pg = new PostgresBackend();
const client = pg.getClient();

socketServer.on('connection', (socket) => {
    console.log(socket.id);
    logger.info(`socket id = ${socket.id}`);
    // upgradedServer.emit("broadcastMessage", data);
    for (let i = 0; i <= 3; i++) {
        setTimeout(() => {
            socketServer.emit('broadcastMessage', {i});
            logger.info(`emitting broadcastMessage i=${i}`)
        }, i * 2000)
    }
    socket.on('sendingMessage', (data) => {
        console.log(data);
    });

    createListener(client, 'new_jobs', (data) => {
        const payload = JSON.parse(data.payload);
        socketServer.emit('broadcastMessage', payload);
    });
})

createListeners(client);

// Handle unhandled promise rejections
process.on('unhandledRejection', (err, promise) => {
    logger.error(`Error: ${err.message}`.red);
});
