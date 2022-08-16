global.__basedir = __dirname;
const express = require('express');
const socket = require('socket.io');
const app = express();
const port = 3001;
const general = require('./routes/general');
const celcom = require('./routes/celcom');
const dnb = require('./routes/dnb');
const dnb2 = require('./routes/dnb2');
const dnb3 = require('./routes/dnb_stats_v3/dnb3');
const tts = require('./tts/tts');
const cors = require('cors');
const PostgresBackend = require("./db/PostgresBackend");
const format = require('pg-format');
const {createListeners} = require("./db/utils");
const {logger} = require("./middleware/logger");
const {isObject} = require("./db/utils");
const errorHandler = require('./middleware/error');
const {createListener} = require("./db/utils");
const {postgrestProxy} = require("./proxies/proxify");
const {createProxyMiddleware} = require("http-proxy-middleware");
const result = require('dotenv').config({path: './.env'})
const {createWatcherProcess} = require("./tools/utils");

logger.info('Starting app.js...');
if (result.error) {
    throw result.error
}
app.use(express.json());
app.use(express.urlencoded({extended: true}));
app.use(cors());
app.use(express.static('static'));
app.use('/node/general', general);
app.use('/node/celcom', celcom);
app.use('/node/dnb', dnb);
app.use('/node/dnb', dnb2);
app.use('/node/dnb/v3', dnb3);
app.use('/node/tts', tts);
app.use(errorHandler);

// Proxy endpoints
// app.use('/node/jlab2', createProxyMiddleware({
//     changeOrigin: true,
//     pathRewrite: {
//         [`^/node/jlab2`]: '',
//     },
//     target: "https://stackoverflow.com/",
//     ws: true
//
// }));

// app.use('/node/pgr', );createProxyMiddleware({
//     changeOrigin: true,
//     prependPath: false,
//     target: "http://localhost:3000",
//     logLevel: 'debug',
//     pathRewrite: {
//         '^/node/pgr': '', // remove base path
//     },
// })


const server = app.listen(port,
    () => {
        console.log(`listening on ${port}`);
        logger.info(
            'starting server....'
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
    logger.error(`Error: ${err.message}`);
    console.log(err);
});


createWatcherProcess();

if (process.platform === 'win32') {


}