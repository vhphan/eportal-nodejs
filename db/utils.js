const PostgresBackend = require("./PostgresBackend");
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

const createDbHistoryListener = (client) => {
    createListener(client, 'db_change', async (data) => {
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
    });
}
// const createJobListener = (client, socketServer) => {
//     createListener(client, 'new_jobs', async (data) => {
//         const payload = JSON.parse(data.payload);
//
//     });
// }


module.exports = {
    dataToCSV,
    getCookies,
    createListener,
    isObject,
    createListeners: createDbHistoryListener
}