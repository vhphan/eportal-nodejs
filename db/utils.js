const PostgresBackend = require("./PostgresBackend");
const {logger} = require("../middleware/logger");
const nodemailer = require("nodemailer");


const roundJsonValues = (jsonArray) => {
    return jsonArray.map(d1 => {
        let d2 = {}
        for (const [key, value] of Object.entries(d1)) {
            if (typeof value === "number") {
                d2[key] = parseFloat(value.toFixed(5));
                continue;
            }
            d2[key] = value;
        }
        return d2;
    })
}

const dataToCSV = (dataList, headers) => {

    let allObjects = [];
    allObjects.push(headers);

    dataList.forEach(function (object) {
        let arr = [];
        arr.push(object.id);
        arr.push(object.term);
        arr.push(object.Date);
        allObjects.push(arr)
    });

    let csvContent = "";
    allObjects.forEach(function (infoArray, index) {
        let dataString = infoArray.join(",");
        csvContent += index < allObjects.length ? dataString + "\n" : dataString;
    });

    return csvContent;
};

const getCookies = request => {
    let cookies = {};
    request.headers && request.headers.cookie && request.headers.cookie.split(';').forEach(function (cookie) {
        let parts = cookie.match(/(.*?)=(.*)$/)
        cookies[parts[1].trim()] = (parts[2] || '').trim();
    });
    return cookies;
};

const createListener = function (pgClient, eventName, callBack = null) {
    pgClient.connect();
    pgClient.query(`LISTEN "${eventName}"`);
    pgClient.on('notification', function (data) {
        if (callBack) callBack(data);
    });
};

const isObject = (v) => typeof v === 'object' && v !== null;

const defaultListenerCallback = async (data) => {
    const payload = JSON.parse(data.payload);
    const newVal = payload['new_val'];
    const oldVal = payload['old_val'];
    let parseResults = [];
    let idObj = {};
    // const idKeys = ['dnb_index', 'SiteName', 'SiteProjectName', 'SectorId', 'System', 'WorkplanID'];
    const idKeys = ['dnb_index', 'SiteName', 'SiteProjectName', 'SectorId', 'System'];

    idKeys.forEach(k => idObj[k] = null);
    Object.entries(newVal).forEach(([key, val]) => {
        if (idKeys.includes(key)) {
            idObj[key] = val;
        }
    });
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
                payload['tstamp'],
                idObj['dnb_index'],
                idObj['SiteName'],
                idObj['SiteProjectName'],
                idObj['SectorId'],
                idObj['System'],
                idObj['WorkplanID'],

            ])
        }
    })
    console.log(parseResults);
    client.query(format('INSERT INTO logging.t_history_parsed (table_name, column_name, old_value, new_value, updated_by, time_stamp, dnb_index, "SiteName", "SiteProjectName", "SectorId", "System", "WorkplanID") VALUES %L', parseResults), [], (err, result) => {
        console.log(err);
        console.log(result);
    });
}

const createDbHistoryListener = (client, eventName = 'db_change', callback = defaultListenerCallback) => {
    createListener(client, eventName, callback);
}
// const createJobListener = (client, socketServer) => {
//     createListener(client, 'new_jobs', async (data) => {
//         const payload = JSON.parse(data.payload);
//
//     });
// }
const sendEmail = function (email, subject, message) {
    logger.info("Sending email to " + email);
    logger.info(email);
    logger.info(message);
    const transporter = nodemailer.createTransport({
        host: 'mail.eprojecttrackers.com',
        port: 465,
        auth: {
            user: 'eri_portal@eprojecttrackers.com',
            pass: process.env.MAIL_PASSWORD
        }
    });
    const mailOptions = {
        from: 'eri_portal@eprojecttrackers.com',
        to: email,
        bcc: 'vee.huen.phan@ericsson.com',
        subject: subject,
        text: message
    }
    transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            console.log(error);
            logger.error(error.message);
        } else {
            console.log('Email sent: ' + info.response);
        }
    });
}

module.exports = {
    dataToCSV,
    getCookies,
    createListener,
    isObject,
    createListeners: createDbHistoryListener,
    roundJsonValues, sendEmail
}