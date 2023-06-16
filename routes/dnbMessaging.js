const express = require("express");
const {createMyLogger} = require("#src/middleware/logger");
const {sqlElephant} = require("#src/db/pgjs/Elephant");
const {sqlDnb} = require("#src/db/pgjs/PgJsBackend14");
const {auth} = require("#src/auth");
const {unless} = require("#src/tools/utils");

const dnbMessagingRouter = express.Router();

// dnbMessagingRouter.use(unless(auth('dnb'), "/"));

const logger = createMyLogger('dnbSocketRouter');

async function insertMessage({
                                 fromServer,
                                 toServer,
                                 jsonMessage,
                                 textMessage
                             }) {

    const sqlBackend = toServer === 'Hawk' ? sqlElephant : sqlDnb;

    const [newRow] = await sqlBackend.begin(async sql => {
        const insertedRow = await sql`
            insert into public.events
            (from_server,
             to_server,
             json_message,
             text_message)
            values (${fromServer}, ${toServer}, ${jsonMessage},
                    ${textMessage}) returning id, from_server, to_server, json_message, text_message
        `;
        return [insertedRow];
    });
    sqlBackend.end().then(() => {
        logger.info('sqlBackend.end()');
    });
    return newRow;
}

dnbMessagingRouter.get('/', (request, response) => {
        response.status(200).json({message: 'hello from dnbMessaging'});
    }
);

dnbMessagingRouter.post('/sendToHawk', async (request, response) => {
        logger.info(`request.body = ${JSON.stringify(request.body)}`);
        const {jsonMessage} = request.body;
        const fromServer = request.body.fromServer || 'ePortal';
        const toServer = request.body.toServer || 'Hawk';
        const textMessage = request.body.textMessage || 'no text message';
        const newRow = await insertMessage({
            fromServer,
            toServer,
            jsonMessage,
            textMessage
        });
        response.json({
            message: newRow,
            success: true
        });
    }
);

dnbMessagingRouter.post('/responseFromHawk', async (request, response) => {

        const fromServer = request.body.fromServer || 'Hawk';
        const toServer = request.body.toServer || 'ePortal';
        const jsonMessage = request.body.jsonMessage || 'no json message';
        const textMessage = request.body.textMessage || 'no text message';

        const message = await insertMessage({
            fromServer,
            toServer,
            jsonMessage,
            textMessage
        });

        response.json({
                success: true,
                message
            }
        );

    }
);


module.exports = {
    dnbMessagingRouter,
};