const express = require("express");
const {logger} = require("#src/middleware/logger");
const dnbSocketRouter = express.Router();

const createSocketServer = (server) => {
    const {Server} = require('socket.io');

    const io = new Server(
        server,

        {
            cors: {
                origin: '*',
            },
            path: '/node/dnbSocket/socket'
        }
    );
    io.on('connection', (socket) => {
        logger.info(`socket id = ${socket.id}`);
        socket.on('sendingMessage', (data) => {
            logger.info(`sending message: ${data}`);
        });
        socket.on('receiveMessage', (data) => {
                logger.info(`received message: ${data}`);
            }
        );
    });
    const sendHeartbeat = () => {
        // io.send(`hello from server. socket id = ${io.id}. The time is ${new Date()}`);
        io.emit('broadcastMessage', `hello from server. The time is ${new Date()}`);
    };
    setInterval(sendHeartbeat, 2000);

    return io;

};

dnbSocketRouter.get('/', (request, response) => {
        response.status(200).json({message: 'hello from dnbSocket'});
    }
);

dnbSocketRouter.get('/dnbWebSocket', (request, response) => {
    //     send html file
    response.sendFile(__dirname + '/dnbWebSocket.html');

});


module.exports = {
    dnbSocketRouter,
    createSocketServer,
};