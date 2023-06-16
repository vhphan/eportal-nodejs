const express = require("express");
const {createMyLogger} = require("#src/middleware/logger");
const {sendEmail} = require("#src/db/utils");
const {unless} = require("#src/tools/utils");
const {auth} = require("#src/auth");
const dnbSocketRouter = express.Router();

dnbSocketRouter.use(unless(auth('dnb'), "/resetCodeTunnel", "/resetCodeTunnel92"));

const logger = createMyLogger('dnbSocketRouter');

let dnbSocketServer = null;
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

        socket.on('ePortalFromDnb', (data) => {
                logger.info(`received ePortalFromDnb: ${JSON.stringify(data)}`);
                sendEmail(process.env.DEFAULT_EMAIL_ADDRESS, 'ePortalFromDnb', data).then(r =>
                    logger.info(`sendEmail result: ${r}`))
                    .catch(e => logger.error(`sendEmail error: ${e}`));
            }
        );


    });
    const sendHeartbeat = () => {
        // io.send(`hello from server. socket id = ${io.id}. The time is ${new Date()}`);
        io.emit('broadcastMessage', `hello from server. The time is ${new Date()}`);
    };
    setInterval(sendHeartbeat, 60 * 1000);


    dnbSocketServer = io;
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

dnbSocketRouter.get('/resetCodeTunnel', (request, response) => {
        dnbSocketServer.emit('ePortalToDnb', `resetCodeTunnel at ${new Date()}`);
        response.json({message: 'resetCodeTunnel sent'});
    }
);

dnbSocketRouter.get('/resetCodeTunnel92', (request, response) => {
        dnbSocketServer.emit('ePortalTo92', `resetCodeTunnel at ${new Date()}`);
        response.json({message: 'resetCodeTunnel sent'});
    }
);

dnbSocketRouter.get('/ePortalToDnbCommands', (request, response) => {
        const command = request.query.command;
        logger.info(`ePortalToDnbCommands: ${command}`);
        dnbSocketServer.emit('ePortalToDnbCommands', command);

        const timeOut = setTimeout(() => {
            dnbSocketServer.off('ePortalToDnbCommandsResults');
            response.json({message: `${command} sent. No response from DNB. Timeout.`});
        }, 10_000);

        dnbSocketServer.on('ePortalToDnbCommandsResults', (data) => {
                clearTimeout(timeOut);
                logger.info(`ePortalToDnbCommandsResults: ${JSON.stringify(data)}`);
                dnbSocketServer.off('ePortalToDnbCommandsResults');
                response.json(data);
            }
        );
    }
);

dnbSocketRouter.get('/ePortalToDnb', (request, response) => {
        const message = request.query.message;
        dnbSocketServer.emit('ePortalToDnb', `${message} at ${new Date()}`);
        response.json({message: `${message} sent`});
    }
);

dnbSocketRouter.get('/ePortalToDnbCurl2', function(req, res, next) {
        const url = req.query.url;
        const opt = {url};
        dnbSocketServer.emit('ePortalToDnbCurl2', JSON.stringify(opt));

        dnbSocketServer.on('ePortalToDnbCurlResult', (data) => {
                logger.info(`ePortalToDnbCurlResult: ${data}`);
                logger.info(`ePortalToDnbCurlResult: ${JSON.stringify(data)}`);
                dnbSocketServer.off('ePortalToDnbCurlResult');
                response.json(data);
            }
        );


    }
);


// dnbSocketRouter.get('/ePortalToDnbCommands', (request, response) => {
//         const command = request.query.command;
//         logger.info(`ePortalToDnbCommands: ${command}`);
//         dnbSocketServer.emit('ePortalToDnbCommands', command);
//         response.json({message: `${command} sent`});
//     }
// );


module.exports = {
    dnbSocketRouter,
    createSocketServer,
};